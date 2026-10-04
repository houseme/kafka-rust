//! Manual CPU measurement of the real Produce request builder and frame encoder.
//! Inputs, decoded semantic checks, JSON output, and summaries stay outside each
//! timed 64-call window. Request/frame allocation, drops, black-box fences, and
//! the byte-count accumulator remain in both revisions' measured work.

use std::fs::OpenOptions;
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use kafka_protocol::messages::{ApiKey, ProduceRequest, RequestHeader};
use kafka_protocol::protocol::{Decodable, HeaderVersion};
use kafka_protocol::records::{
    Compression as RecordCompression, RecordBatchDecoder, TimestampType,
};

use super::{API_VERSION_PRODUCE, ProduceMessageRef, build_produce_request};
use crate::compression::Compression;

const CALLS_PER_WINDOW: usize = 64;
const DEFAULT_SAMPLES: usize = 9;
const WARM_MIN_WORK: Duration = Duration::from_millis(150);
const WARM_MAX_WALL: Duration = Duration::from_secs(2);
const WARM_MAX_WINDOWS: usize = 65_536;
const CLIENT_ID: &str = "produce-encoding-cpu-bench";
const TOPIC: &str = "encoding-bench-topic";
const VALUE: [u8; 64] = [b'v'; 64];

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    records: usize,
    interleaved: bool,
    control: bool,
}

// The six production cases are fixed. Empty input is an additional control;
// both one-record shapes are also controls because their actual inputs match.
const CASES: [Case; 7] = [
    Case {
        name: "empty_control",
        records: 0,
        interleaved: false,
        control: true,
    },
    Case {
        name: "contiguous_1",
        records: 1,
        interleaved: false,
        control: true,
    },
    Case {
        name: "interleaved_1",
        records: 1,
        interleaved: true,
        control: true,
    },
    Case {
        name: "contiguous_32",
        records: 32,
        interleaved: false,
        control: false,
    },
    Case {
        name: "interleaved_32",
        records: 32,
        interleaved: true,
        control: false,
    },
    Case {
        name: "contiguous_1024",
        records: 1024,
        interleaved: false,
        control: false,
    },
    Case {
        name: "interleaved_1024",
        records: 1024,
        interleaved: true,
        control: false,
    },
];

struct Window {
    elapsed_ns: u128,
    encoded_bytes: usize,
}

fn fixture_keys(case: Case) -> Vec<Vec<u8>> {
    (0..case.records)
        .map(|index| format!("key-{index:04}").into_bytes())
        .collect()
}

fn fixture_messages(case: Case, keys: &[Vec<u8>]) -> Vec<ProduceMessageRef<'_>> {
    keys.iter()
        .enumerate()
        .map(|(index, key)| {
            let partition = if case.interleaved { index % 8 } else { 0 };
            (
                TOPIC,
                i32::try_from(partition).unwrap(),
                Some(key.as_slice()),
                Some(VALUE.as_slice()),
                &[][..],
            )
        })
        .collect()
}

fn actual_frame(messages: &[ProduceMessageRef<'_>]) -> Bytes {
    let (header, request) = build_produce_request(
        1,
        CLIENT_ID,
        1,
        30_000,
        Compression::NONE,
        black_box(messages),
    )
    .expect("fixed fixture must build an ordinary Produce request");
    crate::protocol::encode_request_frame(&header, &request, API_VERSION_PRODUCE)
        .expect("fixed fixture must encode a complete Produce frame")
}

fn measure_window(messages: &[ProduceMessageRef<'_>]) -> Window {
    let start = Instant::now();
    let mut encoded_bytes = 0;
    for _ in 0..CALLS_PER_WINDOW {
        let frame = actual_frame(black_box(messages));
        encoded_bytes += frame.len();
        black_box(frame);
    }
    Window {
        elapsed_ns: start.elapsed().as_nanos(),
        encoded_bytes,
    }
}

fn checksum_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(1_099_511_628_211);
    }
    checksum
}

fn verify_actual_frame(messages: &[ProduceMessageRef<'_>], mut frame: Bytes) -> (usize, u64) {
    let byte_count = frame.len();
    assert_eq!(usize::try_from(frame.get_i32()).unwrap(), byte_count - 4);
    let header = RequestHeader::decode(
        &mut frame,
        ProduceRequest::header_version(API_VERSION_PRODUCE),
    )
    .unwrap();
    assert_eq!(header.request_api_key, ApiKey::Produce as i16);
    assert_eq!(header.request_api_version, API_VERSION_PRODUCE);
    assert_eq!(header.correlation_id, 1);
    assert_eq!(header.client_id.unwrap().as_str(), CLIENT_ID);
    assert!(header.unknown_tagged_fields.is_empty());
    let request = ProduceRequest::decode(&mut frame, API_VERSION_PRODUCE).unwrap();
    assert!(frame.is_empty(), "complete request bytes must be consumed");
    assert_eq!(request.acks, 1);
    assert_eq!(request.timeout_ms, 30_000);
    assert_eq!(request.transactional_id, None);
    assert!(request.unknown_tagged_fields.is_empty());
    if messages.is_empty() {
        assert_eq!(request.topic_data.len(), 0);
        return (byte_count, 0);
    }
    assert_eq!(request.topic_data.len(), 1);
    let topic = &request.topic_data[0];
    assert_eq!(topic.name.as_str(), TOPIC);
    assert!(topic.unknown_tagged_fields.is_empty());
    let mut seen_partitions = std::collections::HashSet::new();
    let mut record_count = 0;
    let mut checksum = 0u64;
    for partition in &topic.partition_data {
        assert!(seen_partitions.insert(partition.index));
        assert!(partition.unknown_tagged_fields.is_empty());
        let expected: Vec<_> = messages
            .iter()
            .filter(|message| message.1 == partition.index)
            .collect();
        assert!(!expected.is_empty(), "unexpected topic partition");
        let mut bytes = partition.records.clone().unwrap();
        let batches = RecordBatchDecoder::decode_all(&mut bytes).unwrap();
        assert!(bytes.is_empty(), "all record batch bytes must be consumed");
        assert_eq!(batches.len(), 1, "one batch per topic partition");
        let batch = &batches[0];
        assert_eq!(batch.compression, RecordCompression::None);
        assert_eq!(batch.version, 2);
        assert_eq!(batch.records.len(), expected.len());
        for (index, (record, message)) in batch.records.iter().zip(expected).enumerate() {
            assert_eq!(record.key.as_deref(), message.2);
            assert_eq!(record.value.as_deref(), message.3);
            assert_eq!(record.offset, i64::try_from(index).unwrap());
            assert_eq!(record.sequence, i32::try_from(index).unwrap() - 1);
            assert_eq!(record.timestamp, 0);
            assert_eq!(record.timestamp_type, TimestampType::Creation);
            assert_eq!(record.producer_id, -1);
            assert_eq!(record.producer_epoch, -1);
            assert!(!record.transactional && !record.control && !record.delete_horizon);
            assert!(record.headers.is_empty());
            let mut component = 14_695_981_039_346_656_037;
            component = checksum_bytes(component, record.key.as_deref().unwrap());
            component = checksum_bytes(component, record.value.as_deref().unwrap());
            component = checksum_bytes(component, &partition.index.to_be_bytes());
            component = checksum_bytes(component, &record.offset.to_be_bytes());
            component = checksum_bytes(component, &record.sequence.to_be_bytes());
            // Group iteration is arbitrary; key/value ordering is asserted above,
            // while the semantic checksum is independent of HashMap wire order.
            checksum = checksum.wrapping_add(component);
            record_count += 1;
        }
    }
    assert_eq!(record_count, messages.len());
    assert!(
        messages
            .iter()
            .all(|message| seen_partitions.contains(&message.1))
    );
    (byte_count, checksum)
}

fn adjacent_windows_stable(windows: &[u128]) -> bool {
    windows.len() >= 3
        && windows[windows.len() - 3..]
            .windows(2)
            .all(|pair| pair[0] != 0 && pair[0].abs_diff(pair[1]) * 10 <= pair[0])
}

fn warm_case(
    writer: &mut impl Write,
    case: Case,
    messages: &[ProduceMessageRef<'_>],
    bytes_per_call: usize,
) -> bool {
    let start = Instant::now();
    let mut work_ns = 0u128;
    let mut recent = Vec::with_capacity(3);
    let mut count = 0;
    while count < WARM_MAX_WINDOWS && start.elapsed() < WARM_MAX_WALL {
        let window = measure_window(messages);
        assert_eq!(window.encoded_bytes, bytes_per_call * CALLS_PER_WINDOW);
        work_ns += window.elapsed_ns;
        if recent.len() == 3 {
            recent.remove(0);
        }
        recent.push(window.elapsed_ns);
        let stable = adjacent_windows_stable(&recent);
        writeln!(writer, "{{\"kind\":\"warm_window\",\"case\":\"{}\",\"window\":{},\"calls\":{},\"measured_ns\":{},\"encoded_bytes\":{},\"cumulative_work_ns\":{},\"adjacent_stable\":{}}}", case.name, count, CALLS_PER_WINDOW, window.elapsed_ns, window.encoded_bytes, work_ns, stable).unwrap();
        count += 1;
        let wall_ns = start.elapsed().as_nanos();
        if work_ns >= WARM_MIN_WORK.as_nanos() && stable && wall_ns <= WARM_MAX_WALL.as_nanos() {
            writeln!(writer, "{{\"kind\":\"warm_summary\",\"case\":\"{}\",\"passed\":true,\"windows\":{},\"calls\":{},\"work_ns\":{},\"wall_ns\":{}}}", case.name, count, count * CALLS_PER_WINDOW, work_ns, wall_ns).unwrap();
            return true;
        }
    }
    writeln!(writer, "{{\"kind\":\"warm_summary\",\"case\":\"{}\",\"passed\":false,\"windows\":{},\"calls\":{},\"work_ns\":{},\"wall_ns\":{},\"reason\":\"bounded_prewarm_did_not_reach_minimum_work_and_stability\"}}", case.name, count, count * CALLS_PER_WINDOW, work_ns, start.elapsed().as_nanos()).unwrap();
    false
}

fn measure_case(writer: &mut impl Write, case: Case, samples: usize) -> bool {
    let keys = fixture_keys(case);
    let messages = fixture_messages(case, &keys);
    let (bytes_per_call, checksum) = verify_actual_frame(&messages, actual_frame(&messages));
    writeln!(writer, "{{\"kind\":\"fixture\",\"case\":\"{}\",\"records\":{},\"shape\":\"{}\",\"control\":{},\"bytes_per_call\":{},\"semantic_checksum\":{},\"payload_bytes\":64,\"headers_per_record\":0}}", case.name, case.records, if case.interleaved { "interleaved_mod_8" } else { "contiguous_partition_0" }, case.control, bytes_per_call, checksum).unwrap();
    if !warm_case(writer, case, &messages, bytes_per_call) {
        return false;
    }
    let mut timings = Vec::with_capacity(samples);
    for sample in 0..samples {
        let window = measure_window(&messages);
        assert_eq!(window.encoded_bytes, bytes_per_call * CALLS_PER_WINDOW);
        assert!(window.elapsed_ns > 0);
        assert_eq!(
            verify_actual_frame(&messages, actual_frame(&messages)),
            (bytes_per_call, checksum)
        );
        timings.push(window.elapsed_ns);
        writeln!(writer, "{{\"kind\":\"sample\",\"case\":\"{}\",\"sample\":{},\"calls\":{},\"measured_ns\":{},\"encoded_bytes\":{},\"semantic_checksum\":{}}}", case.name, sample, CALLS_PER_WINDOW, window.elapsed_ns, window.encoded_bytes, checksum).unwrap();
    }
    timings.sort_unstable();
    writeln!(writer, "{{\"kind\":\"case_summary\",\"case\":\"{}\",\"samples\":{},\"calls_per_sample\":{},\"sample_median_ns\":{},\"bytes_per_call\":{},\"records_per_call\":{},\"semantic_checksum\":{},\"control\":{}}}", case.name, samples, CALLS_PER_WINDOW, timings[samples / 2], bytes_per_call, case.records, checksum, case.control).unwrap();
    true
}

#[test]
fn produce_encoding_fixed_fixtures_decode_through_the_real_sdk() {
    for case in CASES {
        let keys = fixture_keys(case);
        let messages = fixture_messages(case, &keys);
        verify_actual_frame(&messages, actual_frame(&messages));
    }
}

#[test]
#[ignore = "manual release CPU benchmark; isolated builds and one serial ABBA pass"]
fn produce_encoding_cpu_bench() {
    assert!(
        !black_box(cfg!(debug_assertions)),
        "run the ignored test with --release"
    );
    let samples =
        std::env::var("KAFKA_PRODUCE_ENCODING_SAMPLES").map_or(DEFAULT_SAMPLES, |value| {
            value
                .parse::<usize>()
                .expect("sample count must be an integer")
        });
    assert!(
        samples >= 3 && samples % 2 == 1,
        "use an odd sample count of at least three"
    );
    let variant = std::env::var("KAFKA_PRODUCE_ENCODING_VARIANT")
        .expect("set variant to baseline or candidate");
    assert!(matches!(variant.as_str(), "baseline" | "candidate"));
    let phase =
        std::env::var("KAFKA_PRODUCE_ENCODING_PHASE").expect("set phase to A1, B1, B2, or A2");
    assert!(matches!(phase.as_str(), "A1" | "B1" | "B2" | "A2"));
    let output = std::env::var_os("KAFKA_PRODUCE_ENCODING_OUTPUT")
        .expect("set an unused absolute JSONL output path");
    assert!(Path::new(&output).is_absolute());
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .expect("output must be new; preserve previous raw evidence");
    let mut writer = BufWriter::new(file);
    writeln!(writer, "{{\"kind\":\"metadata\",\"variant\":\"{variant}\",\"phase\":\"{phase}\",\"samples\":{samples},\"calls_per_window\":{CALLS_PER_WINDOW},\"warm_min_work_ns\":{},\"warm_max_wall_ns\":{},\"warm_max_windows\":{WARM_MAX_WINDOWS},\"adjacent_stability_percent\":10,\"fixed_production_cases\":6,\"additional_empty_control\":true,\"includes_allocation_and_drop\":true,\"includes_frame_byte_counter_and_black_box\":true,\"excludes_fixture_decode_output_and_network\":true}}", WARM_MIN_WORK.as_nanos(), WARM_MAX_WALL.as_nanos()).unwrap();
    let mut passed = true;
    for case in CASES {
        passed &= measure_case(&mut writer, case, samples);
    }
    writeln!(writer, "{{\"kind\":\"complete\",\"prewarm_passed_all_cases\":{passed},\"conclusion_requires_external_single_abba_drift_gate\":true}}").unwrap();
    writer.flush().unwrap();
    assert!(
        passed,
        "prewarm failed; keep raw evidence and do not select a rerun"
    );
}
