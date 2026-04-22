use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustfs_kafka::producer::{Compression, Record};
use std::hint::black_box;

fn bench_record_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_creation");
    for size in [64, 1024, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let value = vec![0u8; size];
            b.iter(|| {
                black_box(Record::from_key_value(
                    "bench-topic",
                    black_box(b"key"),
                    black_box(&value),
                ));
            });
        });
    }
    group.finish();
}

fn bench_compression_enum(c: &mut Criterion) {
    let compressions = [
        Compression::NONE,
        Compression::GZIP,
        Compression::SNAPPY,
        Compression::LZ4,
        Compression::ZSTD,
    ];

    c.bench_function("compression_enum_operations", |b| {
        b.iter(|| {
            for comp in &compressions {
                let _disc = black_box(*comp as i32);
                let _debug = black_box(format!("{:?}", comp));
            }
        });
    });
}

criterion_group!(benches, bench_record_serialization, bench_compression_enum);
criterion_main!(benches);
