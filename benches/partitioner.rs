use criterion::{Criterion, criterion_group, criterion_main};
use rustfs_kafka::producer::{DefaultPartitioner, RoundRobinPartitioner};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::hint::black_box;
use twox_hash::XxHash32;

type MyDefaultPartitioner = DefaultPartitioner<BuildHasherDefault<XxHash32>>;

fn bench_default_partitioner_construction(c: &mut Criterion) {
    c.bench_function("default_partitioner_new", |b| {
        b.iter(|| {
            black_box(MyDefaultPartitioner::default());
        })
    });

    c.bench_function("round_robin_partitioner_new", |b| {
        b.iter(|| {
            black_box(RoundRobinPartitioner::new());
        })
    });
}

fn bench_hash_computation(c: &mut Criterion) {
    let key = b"partition-key-12345";

    c.bench_function("hash_key_xxhash32", |b| {
        b.iter(|| {
            let mut h = BuildHasherDefault::<XxHash32>::default().build_hasher();
            h.write(black_box(key));
            black_box(h.finish());
        })
    });
}

criterion_group!(
    benches,
    bench_default_partitioner_construction,
    bench_hash_computation
);
criterion_main!(benches);
