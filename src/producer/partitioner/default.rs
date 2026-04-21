use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};

use twox_hash::XxHash32;

use crate::client;

use super::Partitioner;

/// The default hasher implementation used of `DefaultPartitioner`.
pub type DefaultHasher = XxHash32;

/// As its name implies `DefaultPartitioner` is the default
/// partitioner for `Producer`.
#[derive(Default)]
pub struct DefaultPartitioner<H = BuildHasherDefault<DefaultHasher>> {
    hash_builder: H,
    cntr: u32,
}

impl DefaultPartitioner {
    /// Creates a new partitioner which will use the given hash
    /// builder to hash message keys.
    pub fn with_hasher<B: BuildHasher>(hash_builder: B) -> DefaultPartitioner<B> {
        DefaultPartitioner {
            hash_builder,
            cntr: 0,
        }
    }

    #[must_use]
    pub fn with_default_hasher<B>() -> DefaultPartitioner<BuildHasherDefault<B>>
    where
        B: Hasher + Default,
    {
        DefaultPartitioner {
            hash_builder: BuildHasherDefault::<B>::default(),
            cntr: 0,
        }
    }
}

impl<H: BuildHasher + Send + Sync> Partitioner for DefaultPartitioner<H> {
    #[allow(unused_variables)]
    fn partition(&mut self, topics: Topics<'_>, rec: &mut client::ProduceMessage<'_, '_>) {
        if rec.partition >= 0 {
            return;
        }
        let Some(partitions) = topics.partitions(rec.topic) else {
            return;
        };

        if let Some(key) = rec.key {
            let num_partitions = partitions.num_all();
            if num_partitions == 0 {
                return;
            }
            let mut h = self.hash_builder.build_hasher();
            h.write(key);
            let bucket = h.finish() % u64::from(num_partitions);
            rec.partition = i32::try_from(bucket).unwrap_or_default();
        } else {
            let avail = partitions.available_ids();
            if !avail.is_empty() {
                rec.partition = avail[self.cntr as usize % avail.len()];
                self.cntr = self.cntr.wrapping_add(1);
            }
        }
    }
}

// --------------------------------------------------------------------
// Re-export Partitions/Topics types for use by other partitioners

/// Producer relevant partition information of a particular topic.
///
/// Intented for use by `Partition`s.
#[derive(Debug)]
pub struct Partitions {
    pub(crate) available_ids: Vec<i32>,
    pub(crate) num_all_partitions: u32,
}

impl Partitions {
    #[inline]
    #[must_use]
    pub fn available_ids(&self) -> &[i32] {
        &self.available_ids
    }

    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn num_available(&self) -> u32 {
        self.available_ids.len() as u32
    }

    #[inline]
    #[must_use]
    pub fn num_all(&self) -> u32 {
        self.num_all_partitions
    }
}

/// A description of available topics and their available partitions.
///
/// Intented for use by `Partitioner`s.
pub struct Topics<'a> {
    partitions: &'a HashMap<String, Partitions>,
}

impl<'a> Topics<'a> {
    pub(crate) fn new(partitions: &'a HashMap<String, Partitions>) -> Topics<'a> {
        Topics { partitions }
    }

    #[inline]
    #[must_use]
    pub fn partitions(&'a self, topic: &str) -> Option<&'a Partitions> {
        self.partitions.get(topic)
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod default_partitioner_tests {
    use std::collections::HashMap;
    use std::hash::{BuildHasherDefault, Hasher};

    use super::{DefaultHasher, DefaultPartitioner, Partitioner, Partitions, Topics};
    use crate::client;

    fn topics_map(topics: Vec<(&str, Partitions)>) -> HashMap<String, Partitions> {
        let mut h = HashMap::new();
        for topic in topics {
            h.insert(topic.0.into(), topic.1);
        }
        h
    }

    fn assert_partitioning<P: Partitioner>(
        topics: &HashMap<String, Partitions>,
        p: &mut P,
        topic: &str,
        key: &str,
    ) -> i32 {
        let mut msg = client::ProduceMessage {
            key: Some(key.as_bytes()),
            value: None,
            topic,
            partition: -1,
            headers: &[],
        };
        p.partition(Topics::new(topics), &mut msg);
        let num_partitions =
            i32::try_from(topics.get(topic).unwrap().num_all()).unwrap_or(i32::MAX);
        assert!(msg.partition >= 0 && msg.partition < num_partitions);
        msg.partition
    }

    #[test]
    fn test_key_partitioning() {
        let h = topics_map(vec![
            (
                "foo",
                Partitions {
                    available_ids: vec![0, 1, 4],
                    num_all_partitions: 5,
                },
            ),
            (
                "bar",
                Partitions {
                    available_ids: vec![0, 1],
                    num_all_partitions: 2,
                },
            ),
        ]);

        let mut p: DefaultPartitioner<BuildHasherDefault<DefaultHasher>> =
            DefaultPartitioner::default();

        let h1 = assert_partitioning(&h, &mut p, "foo", "foo-key");
        let h2 = assert_partitioning(&h, &mut p, "foo", "foo-key");
        assert_eq!(h1, h2);

        let h3 = assert_partitioning(&h, &mut p, "foo", "foo-key");
        let h4 = assert_partitioning(&h, &mut p, "foo", "bar-key");
        assert_ne!(h3, h4);
    }

    #[derive(Default)]
    struct MyCustomHasher(u64);

    impl Hasher for MyCustomHasher {
        fn finish(&self) -> u64 {
            self.0
        }
        fn write(&mut self, bytes: &[u8]) {
            self.0 = u64::from(bytes[0]);
        }
    }

    #[test]
    fn default_partitioner_with_custom_hasher_default() {
        let mut p = DefaultPartitioner::with_default_hasher::<MyCustomHasher>();

        let h = topics_map(vec![
            (
                "confirms",
                Partitions {
                    available_ids: vec![0, 1],
                    num_all_partitions: 2,
                },
            ),
            (
                "contents",
                Partitions {
                    available_ids: vec![0, 1, 9],
                    num_all_partitions: 10,
                },
            ),
        ]);

        let p1 = assert_partitioning(&h, &mut p, "confirms", "A");
        assert_eq!(1, p1);

        let p2 = assert_partitioning(&h, &mut p, "contents", "B");
        assert_eq!(6, p2);
    }
}
