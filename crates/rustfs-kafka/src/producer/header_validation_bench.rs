//! Manual CPU microbenchmark of the public header guard.
//! Fixtures and success assertions stay outside the timed sample. The shared
//! loop's black-box fences, bool count, and Result drop remain in both variants.

use std::hint::black_box;
use std::time::Instant;

use bytes::Bytes;

use super::validate_unique_headers;

const HEADER_COUNTS: [usize; 6] = [0, 1, 2, 4, 8, 16];
const WARMUP: u32 = 10_000;
const SAMPLES: usize = 9;
const CALLS: u32 = 100_000;

fn unique_headers(count: usize) -> Vec<(String, Bytes)> {
    (0..count)
        .map(|index| {
            (
                format!("header-key-{index:02}"),
                Bytes::from_static(b"header-value"),
            )
        })
        .collect()
}

fn verify_fixture(headers: &[(String, Bytes)], expected: usize) {
    assert_eq!(headers.len(), expected);
    for (index, (key, value)) in headers.iter().enumerate() {
        assert_eq!(key, &format!("header-key-{index:02}"));
        assert_eq!(value.as_ref(), b"header-value");
    }
    validate_unique_headers(headers).expect("fixture must have unique headers");
}

fn validation_calls(headers: &[(String, Bytes)], calls: u32) -> u64 {
    let mut successes = 0;
    for _ in 0..calls {
        let result = black_box(validate_unique_headers(black_box(headers)));
        successes += u64::from(result.is_ok());
    }
    successes
}

fn measure_fixture(headers: &[(String, Bytes)], count: usize) {
    verify_fixture(headers, count);
    assert_eq!(validation_calls(headers, WARMUP), u64::from(WARMUP));
    let mut timings = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let start = Instant::now();
        let successes = validation_calls(headers, CALLS);
        let elapsed = start.elapsed();
        assert_eq!(
            successes,
            u64::from(CALLS),
            "invalid-input fast path was measured"
        );
        verify_fixture(headers, count);
        timings.push(elapsed);
        eprintln!(
            "header_validation_cpu_sample headers={count} sample={sample} calls={CALLS} measured_ns={} successes={successes}",
            elapsed.as_nanos()
        );
    }
    timings.sort_unstable();
    let median = timings[SAMPLES / 2].as_nanos();
    eprintln!(
        "header_validation_cpu_summary headers={count} warmup={WARMUP} samples={SAMPLES} calls={CALLS} sample_median_ns={median} ns_per_call={}",
        decimal_rate(median, u128::from(CALLS))
    );
}

fn decimal_rate(numerator: u128, denominator: u128) -> String {
    let whole = numerator / denominator;
    let fraction = (numerator % denominator) * 1_000 / denominator;
    format!("{whole}.{fraction:03}")
}

#[test]
#[ignore = "manual release CPU benchmark; isolated builds and one serial ABBA pass"]
fn header_validation_cpu_bench() {
    assert!(
        !black_box(cfg!(debug_assertions)),
        "run this ignored test with --release"
    );
    let fixtures = HEADER_COUNTS.map(unique_headers);
    for (headers, count) in fixtures.iter().zip(HEADER_COUNTS) {
        measure_fixture(headers, count);
    }
}
