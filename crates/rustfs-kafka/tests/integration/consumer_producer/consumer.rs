use super::*;
use rustfs_kafka::error;
use rustfs_kafka::producer::Record;

/// Tests that consuming one message works
#[test]
fn test_consumer_poll() {
    let topic = unique_test_topic("kafka-rust-consumer-poll");
    create_test_topic(&topic);

    // poll once to set a position in the topic
    let group = unique_test_group();
    let mut consumer = test_consumer_with_topic_and_group(&topic, &group);
    let mut messages = consumer.poll().unwrap();
    assert!(
        messages.is_empty(),
        "messages was not empty: {:?}",
        messages
    );

    // send a message and then poll it and ensure it is the correct message
    let correct_message_contents = "test_consumer_poll".as_bytes();
    let mut producer = test_producer();
    producer
        .send(&Record::from_value(&topic, correct_message_contents))
        .unwrap();

    const MAX_POLL_ATTEMPTS: usize = 100;
    let mut message_set = None;
    for attempt in 0..MAX_POLL_ATTEMPTS {
        messages = consumer.poll().unwrap();
        if let Some(ms) = messages.iter().next()
            && !ms.messages().is_empty()
        {
            message_set = Some(ms);
            break;
        }
        if attempt % 10 == 0 {
            debug!(
                "still waiting for produced message in test_consumer_poll: attempt={}",
                attempt
            );
        }
    }
    let message_set = message_set.expect("timed out waiting for message in test_consumer_poll");

    assert_eq!(
        1,
        message_set.messages().len(),
        "should only be one message"
    );

    let message_content = &message_set.messages()[0].value;
    assert_eq!(
        correct_message_contents, message_content,
        "incorrect message contents"
    );
}

/// Test Consumer::commit_messageset
#[test]
fn test_consumer_commit_messageset() {
    let topic = unique_test_topic("kafka-rust-commit-messageset");
    create_test_topic(&topic);

    let group = unique_test_group();
    let mut consumer = test_consumer_with_topic_and_group(&topic, &group);

    // get the offsets at the beginning of the test
    let start_offsets = get_group_offsets(&mut new_ready_kafka_client(), &group, &topic, Some(0));

    debug!("start offsets: {:?}", start_offsets);

    // poll once to set a position in the topic
    let messages = consumer.poll().unwrap();
    assert!(
        messages.is_empty(),
        "messages was not empty: {:?}",
        messages
    );

    // send some messages to the topic
    const NUM_MESSAGES_I64: i64 = 100;
    const NUM_MESSAGES_U32: u32 = 100;
    const NUM_MESSAGES_USIZE: usize = 100;
    send_random_messages(&mut test_producer(), &topic, NUM_MESSAGES_U32);

    let mut num_messages = 0;

    const MAX_POLL_ATTEMPTS: usize = 2000;
    let mut reached_target = false;
    for attempt in 0..MAX_POLL_ATTEMPTS {
        for ms in consumer.poll().unwrap().iter() {
            let messages = ms.messages();
            num_messages += messages.len();
            consumer.consume_messageset(&ms).unwrap();
        }

        consumer.commit_consumed().unwrap();

        if num_messages >= NUM_MESSAGES_USIZE {
            reached_target = true;
            break;
        }

        if attempt % 50 == 0 {
            debug!(
                "still waiting for messages in test_consumer_commit_messageset: attempt={}, observed={}",
                attempt, num_messages
            );
        }
        poll_backoff(20);
    }

    assert!(
        reached_target,
        "timed out waiting for consumed messages: expected at least {}, got {}",
        NUM_MESSAGES_USIZE, num_messages
    );

    assert_eq!(NUM_MESSAGES_USIZE, num_messages, "wrong number of messages");

    // get the latest offsets and make sure they add up to the number of messages
    let latest_offsets = get_group_offsets(&mut new_ready_kafka_client(), &group, &topic, Some(0));

    debug!("end offsets: {:?}", latest_offsets);

    // add up the differences
    let num_new_messages_committed = diff_group_offsets(&start_offsets, &latest_offsets);

    assert_eq!(
        NUM_MESSAGES_I64, num_new_messages_committed,
        "wrong number of messages committed"
    );

    for partition in consumer.subscriptions().get(&topic).unwrap() {
        let consumed_offset = consumer.last_consumed_message(&topic, *partition).unwrap();
        let latest_offset = latest_offsets.get(partition).unwrap();
        assert_eq!(
            *latest_offset - 1,
            consumed_offset,
            "latest consumed offset is incorrect"
        );
    }
}

/// Verify that if Consumer::commit_consumed is called without consuming any
/// message sets, nothing is committed.
#[test]
fn test_consumer_commit_messageset_no_consumes() {
    let topic = unique_test_topic("kafka-rust-commit-no-consume");
    create_test_topic(&topic);

    let group = unique_test_group();
    let mut consumer = test_consumer_with_topic_and_group(&topic, &group);

    // get the offsets at the beginning of the test
    let start_offsets = get_group_offsets(&mut new_ready_kafka_client(), &group, &topic, Some(0));

    debug!("start offsets: {:?}", start_offsets);

    // poll once to set a position in the topic
    let messages = consumer.poll().unwrap();
    assert!(
        messages.is_empty(),
        "messages was not empty: {:?}",
        messages
    );

    // send some messages to the topic
    const NUM_MESSAGES_U32: u32 = 100;
    const NUM_MESSAGES_USIZE: usize = 100;
    send_random_messages(&mut test_producer(), &topic, NUM_MESSAGES_U32);

    let mut num_messages = 0;

    const MAX_POLL_ATTEMPTS: usize = 2000;
    let mut reached_target = false;
    for attempt in 0..MAX_POLL_ATTEMPTS {
        for ms in consumer.poll().unwrap().iter() {
            let messages = ms.messages();
            num_messages += messages.len();

            // DO NOT consume the messages
            // consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().unwrap();

        if num_messages >= NUM_MESSAGES_USIZE {
            reached_target = true;
            break;
        }

        if attempt % 50 == 0 {
            debug!(
                "still waiting for messages in test_consumer_commit_messageset_no_consumes: attempt={}, observed={}",
                attempt, num_messages
            );
        }
        poll_backoff(20);
    }

    assert!(
        reached_target,
        "timed out waiting for consumed messages: expected at least {}, got {}",
        NUM_MESSAGES_USIZE, num_messages
    );

    assert_eq!(NUM_MESSAGES_USIZE, num_messages, "wrong number of messages");

    // get the latest offsets and make sure they add up to the number of messages
    let latest_offsets = get_group_offsets(&mut consumer.into_client(), &group, &topic, Some(0));

    debug!("end offsets: {:?}", latest_offsets);

    // add up the differences
    let num_new_messages_committed = diff_group_offsets(&start_offsets, &latest_offsets);

    // without consuming any messages, the diff should be 0
    assert_eq!(
        0, num_new_messages_committed,
        "wrong number of messages committed"
    );
}

/// Consuming from a non-existent topic should fail.
#[test]
fn test_consumer_non_existent_topic() {
    let consumer_err = test_consumer_builder()
        .with_topic_partitions("foo_topic".to_owned(), &TEST_TOPIC_PARTITIONS)
        .create()
        .unwrap_err();

    let error::Error::Kafka(error_code) = consumer_err else {
        panic!("Should have received Kafka error");
    };

    let correct_error_code = error::KafkaCode::UnknownTopicOrPartition;
    assert_eq!(
        correct_error_code, error_code,
        "should have errored on non-existent topic"
    );
}
