use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, OffsetCommitRequest, OffsetCommitResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

use super::{
    OffsetRequestContext, commit_offsets_inner, fetch_group_offsets_inner,
    validate_offset_commit_acknowledgements, validate_offset_fetch_acknowledgements,
};
use crate::client::{KafkaClient, RetryPolicy};
use crate::error::{Error, KafkaCode};
use crate::protocol::consumer::{
    OffsetCommitResponse as ConvertedResponse, PartitionOffsetCommitResponse,
    TopicPartitionOffsetCommitResponse,
};

fn converted(rows: &[(&str, i32, i16)]) -> ConvertedResponse {
    let mut topic_partitions: Vec<TopicPartitionOffsetCommitResponse> = Vec::new();
    for &(topic, partition, error) in rows {
        let index = topic_partitions
            .iter()
            .position(|entry| entry.topic == topic)
            .unwrap_or_else(|| {
                topic_partitions.push(TopicPartitionOffsetCommitResponse {
                    topic: topic.to_owned(),
                    partitions: Vec::new(),
                });
                topic_partitions.len() - 1
            });
        topic_partitions[index]
            .partitions
            .push(PartitionOffsetCommitResponse { partition, error });
    }
    ConvertedResponse { topic_partitions }
}

#[test]
fn offset_commit_requires_exactly_one_ack_for_every_requested_partition() {
    let expected = HashSet::from([("a", 0), ("a", 1), ("b", 0)]);
    for rows in [
        vec![],
        vec![("a", 0, 0)],
        vec![("a", 0, 0), ("a", 1, 0), ("a", 1, 0), ("b", 0, 0)],
        vec![("a", 0, 0), ("a", 1, 0), ("b", 0, 0), ("extra", 0, 0)],
        vec![("a", 0, 0), ("a", 1, 0), ("b", 9, 0)],
    ] {
        assert!(validate_offset_commit_acknowledgements(&converted(&rows), &expected).is_err());
    }
    let valid = converted(&[("b", 0, 0), ("a", 1, 16), ("a", 0, 0)]);
    assert!(validate_offset_commit_acknowledgements(&valid, &expected).is_ok());
    let mut extra = converted(&[("a", 0, 0), ("a", 1, 0), ("b", 0, 0)]);
    extra
        .topic_partitions
        .push(TopicPartitionOffsetCommitResponse {
            topic: "extra".into(),
            partitions: vec![],
        });
    assert!(validate_offset_commit_acknowledgements(&extra, &expected).is_err());
}

fn response(rows: &[(&str, i32, i16)]) -> OffsetCommitResponse {
    use kafka_protocol::messages::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    let mut topics: Vec<OffsetCommitResponseTopic> = Vec::new();
    for &(topic, partition, error) in rows {
        let index = topics
            .iter()
            .position(|entry| entry.name.as_str() == topic)
            .unwrap_or_else(|| {
                topics.push(
                    OffsetCommitResponseTopic::default()
                        .with_name(StrBytes::from_string(topic.to_owned()).into()),
                );
                topics.len() - 1
            });
        topics[index].partitions.push(
            OffsetCommitResponsePartition::default()
                .with_partition_index(partition)
                .with_error_code(error),
        );
    }
    OffsetCommitResponse::default().with_topics(topics)
}

fn wire_commit(rows: Vec<(&'static str, i32, i16)>) -> crate::error::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut bytes = vec![0; usize::try_from(i32::from_be_bytes(length)).unwrap()];
        stream.read_exact(&mut bytes).unwrap();
        let version = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let mut bytes = Bytes::from(bytes);
        let header =
            RequestHeader::decode(&mut bytes, OffsetCommitRequest::header_version(version))
                .unwrap();
        assert_eq!(header.request_api_key, ApiKey::OffsetCommit as i16);
        let request = OffsetCommitRequest::decode(&mut bytes, version).unwrap();
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(request.topics[0].partitions[0].committed_offset, 42);
        let response = response(&rows);
        let mut bytes = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(header.correlation_id)
            .encode(&mut bytes, OffsetCommitResponse::header_version(version))
            .unwrap();
        response.encode(&mut bytes, version).unwrap();
        stream
            .write_all(&i32::try_from(bytes.len()).unwrap().to_be_bytes())
            .unwrap();
        stream.write_all(&bytes).unwrap();
        // Even a malformed success must not cause an automatic OffsetCommit replay.
        match stream.read(&mut length) {
            Ok(0) => {}
            Err(error) if matches!(error.kind(), std::io::ErrorKind::ConnectionReset) => {}
            other => panic!("unexpected commit replay: {other:?}"),
        }
    });
    let mut client = KafkaClient::builder()
        .with_conn_rw_timeout(2)
        .with_retry_policy(RetryPolicy::Fixed {
            interval: Duration::ZERO,
            max_attempts: 3,
        })
        .build();
    let _ = client.state.set_group_coordinator(
        "group",
        &crate::protocol::consumer::GroupCoordinatorResponse {
            broker_id: 1,
            host: address.ip().to_string(),
            port: i32::from(address.port()),
            ..Default::default()
        },
    );
    let mut context = OffsetRequestContext {
        correlation_id: 7,
        client_id: &client.config.client_id,
        state: &mut client.state,
        conn_pool: &mut client.conn_pool,
        config: &client.config,
        api_versions: &client.api_versions,
    };
    let result = commit_offsets_inner(
        &[("a", 0, 42, None), ("a", 1, 9, None)],
        "group",
        &mut context,
    );
    drop(client);
    server.join().unwrap();
    result
}

#[test]
fn malformed_commit_success_is_rejected_without_replay() {
    for rows in [
        vec![],
        vec![("a", 0, 0)],
        vec![("a", 0, 0), ("a", 0, 0), ("a", 1, 0)],
        vec![("a", 0, 0), ("a", 1, 0), ("extra", 0, 0)],
    ] {
        assert!(wire_commit(rows).is_err());
    }
}

#[test]
fn complete_commit_errors_preserve_the_original_kafka_failure() {
    assert!(matches!(
        wire_commit(vec![
            ("a", 1, KafkaCode::GroupAuthorizationFailed as i16),
            ("a", 0, 0)
        ]),
        Err(Error::Kafka(KafkaCode::GroupAuthorizationFailed))
    ));
}

#[test]
fn duplicate_commit_inputs_fail_before_coordinator_or_broker_io() {
    let mut client = KafkaClient::new(vec![]);
    let mut context = OffsetRequestContext {
        correlation_id: 1,
        client_id: "",
        state: &mut client.state,
        conn_pool: &mut client.conn_pool,
        config: &client.config,
        api_versions: &client.api_versions,
    };
    assert!(matches!(
        commit_offsets_inner(
            &[("a", 0, 1, None), ("a", 0, 2, None)],
            "group",
            &mut context
        ),
        Err(Error::Config(_))
    ));
}

fn fetched(rows: &[(&str, i32, i64)]) -> kafka_protocol::messages::OffsetFetchResponse {
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    let mut topics: Vec<OffsetFetchResponseTopic> = Vec::new();
    for &(topic, partition, offset) in rows {
        let index = topics
            .iter()
            .position(|entry| entry.name.as_str() == topic)
            .unwrap_or_else(|| {
                topics.push(
                    OffsetFetchResponseTopic::default()
                        .with_name(StrBytes::from_string(topic.to_owned()).into()),
                );
                topics.len() - 1
            });
        topics[index].partitions.push(
            OffsetFetchResponsePartition::default()
                .with_partition_index(partition)
                .with_committed_offset(offset),
        );
    }
    kafka_protocol::messages::OffsetFetchResponse::default().with_topics(topics)
}

#[test]
fn successful_offset_fetch_requires_explicit_acknowledgements_and_valid_committed_offsets() {
    let expected = HashSet::from([("a", 0), ("a", 1)]);
    for rows in [
        vec![],
        vec![("a", 0, -1)],
        vec![("a", 0, -1), ("a", 0, 0), ("a", 1, 42)],
        vec![("a", 0, 0), ("extra", 1, 42)],
        vec![("a", 0, -2), ("a", 1, 42)],
        vec![("a", 0, i64::MIN), ("a", 1, 42)],
    ] {
        assert!(validate_offset_fetch_acknowledgements(&fetched(&rows), &expected).is_err());
    }
    for offset in [-1, 0, i64::MAX] {
        assert!(
            validate_offset_fetch_acknowledgements(
                &fetched(&[("a", 1, offset), ("a", 0, 42)]),
                &expected
            )
            .is_ok()
        );
    }
}

#[test]
fn repeated_commit_topic_wrappers_are_rejected_even_with_disjoint_or_empty_partitions() {
    let expected = HashSet::from([("a", 0), ("a", 1)]);
    let mut disjoint = converted(&[("a", 0, 0)]);
    disjoint
        .topic_partitions
        .extend(converted(&[("a", 1, 0)]).topic_partitions);
    let mut repeated_empty = converted(&[("a", 0, 0), ("a", 1, 0)]);
    repeated_empty
        .topic_partitions
        .push(TopicPartitionOffsetCommitResponse {
            topic: "a".into(),
            partitions: vec![],
        });
    for response in [disjoint, repeated_empty] {
        assert!(validate_offset_commit_acknowledgements(&response, &expected).is_err());
    }
}

fn repeated_fetch_topic_response(disjoint: bool) -> kafka_protocol::messages::OffsetFetchResponse {
    let mut response = if disjoint {
        fetched(&[("a", 0, 42)])
    } else {
        fetched(&[("a", 0, 42), ("a", 1, 99)])
    };
    if disjoint {
        response.topics.extend(fetched(&[("a", 1, 99)]).topics);
    } else {
        response.topics.push(
            kafka_protocol::messages::offset_fetch_response::OffsetFetchResponseTopic::default()
                .with_name(StrBytes::from_static_str("a").into()),
        );
    }
    response
}

#[test]
fn repeated_fetch_topic_wrappers_are_rejected_before_the_output_map_can_overwrite_offsets() {
    let expected = HashSet::from([("a", 0), ("a", 1)]);
    for disjoint in [false, true] {
        assert!(
            validate_offset_fetch_acknowledgements(
                &repeated_fetch_topic_response(disjoint),
                &expected
            )
            .is_err()
        );
    }
    let mut extra_empty = fetched(&[("a", 0, 42), ("a", 1, 99)]);
    extra_empty.topics.push(
        kafka_protocol::messages::offset_fetch_response::OffsetFetchResponseTopic::default()
            .with_name(StrBytes::from_static_str("extra").into()),
    );
    assert!(validate_offset_fetch_acknowledgements(&extra_empty, &expected).is_err());
}

#[test]
fn repeated_fetch_topic_wrappers_close_the_connection_without_replay_or_fallback() {
    use crate::error::ProtocolError;
    use kafka_protocol::messages::{OffsetFetchRequest, OffsetFetchResponse};

    for disjoint in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let fallback = TcpListener::bind("127.0.0.1:0").unwrap();
        fallback.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut length = [0; 4];
            stream.read_exact(&mut length).unwrap();
            let mut payload = vec![0; usize::try_from(i32::from_be_bytes(length)).unwrap()];
            stream.read_exact(&mut payload).unwrap();
            let version = i16::from_be_bytes(payload[2..4].try_into().unwrap());
            let mut payload = Bytes::from(payload);
            let header =
                RequestHeader::decode(&mut payload, OffsetFetchRequest::header_version(version))
                    .unwrap();
            assert_eq!(header.request_api_key, ApiKey::OffsetFetch as i16);
            let request = OffsetFetchRequest::decode(&mut payload, version).unwrap();
            assert_eq!(request.group_id.as_str(), "group");
            let topics = request.topics.unwrap();
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].partition_indexes, [0, 1]);
            assert!(payload.is_empty());
            let response = repeated_fetch_topic_response(disjoint);
            let mut payload = BytesMut::new();
            ResponseHeader::default()
                .with_correlation_id(header.correlation_id)
                .encode(&mut payload, OffsetFetchResponse::header_version(version))
                .unwrap();
            response.encode(&mut payload, version).unwrap();
            stream
                .write_all(&i32::try_from(payload.len()).unwrap().to_be_bytes())
                .unwrap();
            stream.write_all(&payload).unwrap();
            // The caller keeps its client alive until this check completes.
            // EOF therefore proves explicit retirement, not a dropped fixture.
            let mut byte = [0];
            match stream.read(&mut byte) {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                other => panic!("malformed OffsetFetch was reused or replayed: {other:?}"),
            }
            listener.set_nonblocking(true).unwrap();
            assert!(
                matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
                "malformed OffsetFetch triggered a replacement connection"
            );
        });
        let mut client = KafkaClient::builder()
            .with_hosts(vec![
                address.to_string(),
                fallback.local_addr().unwrap().to_string(),
            ])
            .with_conn_rw_timeout(2)
            .with_retry_policy(RetryPolicy::Fixed {
                interval: Duration::ZERO,
                max_attempts: 3,
            })
            .build();
        client.state.set_group_coordinator(
            "group",
            &crate::protocol::consumer::GroupCoordinatorResponse {
                broker_id: 1,
                host: address.ip().to_string(),
                port: i32::from(address.port()),
                ..Default::default()
            },
        );
        let mut context = OffsetRequestContext {
            correlation_id: 7,
            client_id: &client.config.client_id,
            state: &mut client.state,
            conn_pool: &mut client.conn_pool,
            config: &client.config,
            api_versions: &client.api_versions,
        };
        let result = fetch_group_offsets_inner(&[("a", 0), ("a", 1)], "group", &mut context);
        assert!(
            matches!(result, Err(Error::BrokerRequestError { api_key: "OffsetFetch", source, .. })
            if matches!(source.as_ref(), Error::Protocol(ProtocolError::Codec)))
        );
        server.join().unwrap();
        assert!(
            matches!(fallback.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }
}
