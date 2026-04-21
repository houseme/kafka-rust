use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::hint::black_box;
use rustfs_kafka::producer::{Record, Headers};

fn bench_record_creation(c: &mut Criterion) {
    c.bench_function("record_from_value", |b| {
        b.iter(|| {
            black_box(Record::from_value("bench-topic", black_box(b"hello world")));
        })
    });

    c.bench_function("record_from_key_value", |b| {
        b.iter(|| {
            black_box(Record::from_key_value("bench-topic", black_box(b"key"), black_box(b"value")));
        })
    });

    c.bench_function("record_with_headers", |b| {
        b.iter(|| {
            let mut headers = Headers::new();
            headers.insert("trace-id", b"abc-123-def");
            headers.insert("content-type", b"application/json");
            black_box(Record::from_key_value("bench-topic", b"key", b"value").with_headers(headers));
        })
    });
}

fn bench_batch_record_creation(c: &mut Criterion) {
    let sizes = [10, 100, 1000];
    let mut group = c.benchmark_group("batch_record_creation");
    for size in sizes {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut records = Vec::with_capacity(size);
                for i in 0..size {
                    let value = format!("msg-{}", i).into_bytes();
                    records.push(Record::from_value("bench-topic", value));
                }
                black_box(records)
            });
        });
    }
    group.finish();
}

fn bench_headers(c: &mut Criterion) {
    c.bench_function("headers_insert_3", |b| {
        b.iter(|| {
            let mut headers = Headers::new();
            headers.insert("key1", b"value1");
            headers.insert("key2", b"value2");
            headers.insert("key3", b"value3");
            black_box(headers);
        })
    });
}

criterion_group!(benches, bench_record_creation, bench_batch_record_creation, bench_headers);
criterion_main!(benches);
