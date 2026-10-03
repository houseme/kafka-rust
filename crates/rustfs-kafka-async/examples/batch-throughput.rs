//! Compare sequential sends with native batching against one Kafka partition.
//!
//! Usage: cargo run -p rustfs-kafka-async --release --example batch-throughput --
//!        localhost:9092 existing-topic [records-per-batch] [rounds]

use std::time::{Duration, Instant};

use rustfs_kafka::producer::{Record, RequiredAcks};
use rustfs_kafka_async::AsyncProducer;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(2..=4).contains(&args.len()) {
        return Err("usage: batch-throughput HOST TOPIC [RECORDS_PER_BATCH] [ROUNDS]".into());
    }
    let count = args.get(2).map_or(Ok(128u32), |s| s.parse())?;
    let rounds = args.get(3).map_or(Ok(16u32), |s| s.parse())?;
    if count == 0 || rounds == 0 || count > 16_384 || rounds > 4096 {
        return Err("records must be 1..=16384 and rounds must be 1..=4096".into());
    }
    let total = count.checked_mul(rounds).ok_or("record count overflow")?;
    let payload = vec![42u8; 1024];
    let records: Vec<_> = (0..count)
        .map(|_| Record::from_value(args[1].as_str(), payload.as_slice()).with_partition(0))
        .collect();
    let producer = AsyncProducer::builder(vec![args[0].clone()])
        .with_required_acks(RequiredAcks::All)
        .build()
        .await?;

    // Warm metadata, connection, encoder, and broker before timing either mode.
    producer.send_all(&records).await?;
    let warmup_rounds = rounds.saturating_mul(4).min(64);
    window(&producer, &records, warmup_rounds, false).await?;
    window(&producer, &records, warmup_rounds, true).await?;

    let a1 = window(&producer, &records, rounds, false).await?;
    let b1 = window(&producer, &records, rounds, true).await?;
    let b2 = window(&producer, &records, rounds, true).await?;
    let a2 = window(&producer, &records, rounds, false).await?;
    producer.close().await?;

    println!(
        "records_per_batch={count} rounds={rounds} warmup_rounds={warmup_rounds} payload_bytes=1024 acks=all partition=0"
    );
    for (label, time) in [("A1", a1), ("B1", b1), ("B2", b2), ("A2", a2)] {
        println!(
            "{label} seconds={:.6} records_per_second={:.2}",
            time.as_secs_f64(),
            f64::from(total) / time.as_secs_f64()
        );
    }
    let drift = (a2.as_secs_f64() / a1.as_secs_f64() - 1.0).abs();
    println!("baseline_drift_percent={:.2}", drift * 100.0);
    if drift > 0.15 {
        println!("comparison=inconclusive baseline drift exceeds 15 percent");
    } else {
        let sequential = a1.as_secs_f64() + a2.as_secs_f64();
        let batched = b1.as_secs_f64() + b2.as_secs_f64();
        println!("comparison=valid batch_speedup={:.2}", sequential / batched);
    }
    Ok(())
}

async fn window<K, V>(
    producer: &AsyncProducer,
    records: &[Record<'_, K, V>],
    rounds: u32,
    batched: bool,
) -> rustfs_kafka::Result<Duration>
where
    K: rustfs_kafka::producer::AsBytes,
    V: rustfs_kafka::producer::AsBytes,
{
    let start = Instant::now();
    for _ in 0..rounds {
        if batched {
            producer.send_all(records).await?;
        } else {
            for record in records {
                producer.send(record).await?;
            }
        }
    }
    Ok(start.elapsed())
}
