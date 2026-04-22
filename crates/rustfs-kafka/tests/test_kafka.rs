#[cfg(feature = "integration_tests")]
extern crate rustfs_kafka;

#[cfg(feature = "integration_tests")]
extern crate rand;

#[cfg(feature = "integration_tests")]
mod integration {
    use temp_env::with_var;
    use tracing::debug;

    use rustfs_kafka::client::{Compression, GroupOffsetStorage, KafkaClient, SecurityConfig};

    mod client;
    mod consumer_producer;

    #[ctor::ctor]
    fn init_tracing() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    pub const LOCAL_KAFKA_BOOTSTRAP_HOST: &str = "localhost:9092";
    pub const TEST_TOPIC_NAME: &str = "kafka-rust-test";
    pub const TEST_TOPIC_NAME_2: &str = "kafka-rust-test2";
    pub const TEST_GROUP_NAME: &str = "kafka-rust-tester";
    pub const TEST_TOPIC_PARTITIONS: [i32; 2] = [0, 1];
    pub const KAFKA_CONSUMER_OFFSETS_TOPIC_NAME: &str = "__consumer_offsets";

    const KAFKA_CLIENT_SECURE: &str = "KAFKA_CLIENT_SECURE";
    const KAFKA_CLIENT_COMPRESSION: &str = "KAFKA_CLIENT_COMPRESSION";

    fn parse_compression(s: &str) -> Compression {
        match s.to_uppercase().as_str() {
            "" | "NONE" => Compression::NONE,
            "GZIP" => Compression::GZIP,
            "SNAPPY" => Compression::SNAPPY,
            other => panic!("Unknown compression type: {other}"),
        }
    }

    /// Constructs a Kafka client for the integration tests, and loads
    /// its metadata so it is ready to use.
    pub(crate) fn new_ready_kafka_client() -> KafkaClient {
        let mut client = new_kafka_client();
        client.load_metadata_all().expect("failed to load metadata");
        client
    }

    /// Constructs a Kafka client for the integration tests.
    pub(crate) fn new_kafka_client() -> KafkaClient {
        let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];

        let mut client = if let Some(security_config) = new_security_config() {
            KafkaClient::new_secure(hosts, security_config)
        } else {
            KafkaClient::new(hosts)
        };

        client.set_group_offset_storage(Some(GroupOffsetStorage::Kafka));

        let compression = std::env::var(KAFKA_CLIENT_COMPRESSION).unwrap_or_default();
        client.set_compression(parse_compression(&compression));
        debug!("Constructing client: {:?}", client);

        client
    }

    /// Returns a new security config if the `KAFKA_CLIENT_SECURE`
    /// environment variable is set to a non-empty string.
    pub(crate) fn new_security_config() -> Option<SecurityConfig> {
        let val = std::env::var(KAFKA_CLIENT_SECURE).ok()?;
        if val.is_empty() {
            return None;
        }
        Some(get_security_config())
    }

    /// If the `KAFKA_CLIENT_SECURE` environment variable is set, return a
    /// `SecurityConfig`.
    pub(crate) fn get_security_config() -> SecurityConfig {
        SecurityConfig::new().with_hostname_verification(false)
    }

    #[test]
    fn test_parse_compression_from_temp_env() {
        with_var(KAFKA_CLIENT_COMPRESSION, Some("gzip"), || {
            let compression = std::env::var(KAFKA_CLIENT_COMPRESSION).unwrap_or_default();
            assert_eq!(Compression::GZIP, parse_compression(&compression));
        });

        with_var(KAFKA_CLIENT_COMPRESSION, Some("snappy"), || {
            let compression = std::env::var(KAFKA_CLIENT_COMPRESSION).unwrap_or_default();
            assert_eq!(Compression::SNAPPY, parse_compression(&compression));
        });

        with_var(KAFKA_CLIENT_COMPRESSION, Some(""), || {
            let compression = std::env::var(KAFKA_CLIENT_COMPRESSION).unwrap_or_default();
            assert_eq!(Compression::NONE, parse_compression(&compression));
        });
    }

    #[test]
    fn test_new_security_config_from_temp_env() {
        with_var(KAFKA_CLIENT_SECURE, None::<&str>, || {
            assert!(
                new_security_config().is_none(),
                "security config should be absent when env is unset"
            );
        });

        with_var(KAFKA_CLIENT_SECURE, Some(""), || {
            assert!(
                new_security_config().is_none(),
                "security config should be absent when env is empty"
            );
        });

        with_var(KAFKA_CLIENT_SECURE, Some("secure"), || {
            assert!(
                new_security_config().is_some(),
                "security config should exist when env is non-empty"
            );
        });
    }
}
