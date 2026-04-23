#[cfg(feature = "integration_tests")]
mod integration {
    use rustfs_kafka::client::{RequiredAcks, SaslConfig, SecurityConfig};
    use rustfs_kafka::producer::Record;
    use rustfs_kafka_async::{AsyncProducer, AsyncProducerConfig};

    const LOCAL_KAFKA_BOOTSTRAP_HOST: &str = "127.0.0.1:9092";
    const TEST_TOPIC_NAME: &str = "kafka-rust-test";

    const KAFKA_CLIENT_SECURE: &str = "KAFKA_CLIENT_SECURE";
    const KAFKA_CLIENT_SASL_MECHANISM: &str = "KAFKA_CLIENT_SASL_MECHANISM";
    const KAFKA_CLIENT_SASL_USERNAME: &str = "KAFKA_CLIENT_SASL_USERNAME";
    const KAFKA_CLIENT_SASL_PASSWORD: &str = "KAFKA_CLIENT_SASL_PASSWORD";

    fn security_from_env() -> Option<SecurityConfig> {
        let secure_val = std::env::var(KAFKA_CLIENT_SECURE).ok()?;
        if secure_val.is_empty() {
            return None;
        }

        let mut config = SecurityConfig::new().with_hostname_verification(false);

        let mechanism = std::env::var(KAFKA_CLIENT_SASL_MECHANISM).unwrap_or_default();
        if !mechanism.is_empty() {
            let username =
                std::env::var(KAFKA_CLIENT_SASL_USERNAME).unwrap_or_else(|_| "test".to_owned());
            let password = std::env::var(KAFKA_CLIENT_SASL_PASSWORD)
                .unwrap_or_else(|_| "test-pass".to_owned());
            config = config.with_sasl(SaslConfig::new(mechanism, username, password));
        }

        Some(config)
    }

    #[tokio::test]
    async fn test_async_producer_send_with_secure_profile() {
        let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];
        let mut producer_config = AsyncProducerConfig::new().with_required_acks(RequiredAcks::One);
        if let Some(security) = security_from_env() {
            producer_config = producer_config.with_security(security);
        }

        let producer = AsyncProducer::from_hosts_with_config(hosts, producer_config)
            .await
            .expect("failed to create async producer with secure profile");

        let payload = format!(
            "secure-sasl-e2e:{}",
            std::env::var(KAFKA_CLIENT_SASL_MECHANISM).unwrap_or_else(|_| "TLS".to_owned())
        );
        let record = Record::from_value(TEST_TOPIC_NAME, payload.as_bytes());

        producer
            .send(&record)
            .await
            .expect("failed to produce message with secure profile");
    }
}
