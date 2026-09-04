#![allow(clippy::wildcard_imports)]
//! Client quota administration helpers.

use kafka_protocol::messages::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, ApiKey, DescribeClientQuotasRequest,
    DescribeClientQuotasResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{API_VERSION_ALTER_CLIENT_QUOTAS, API_VERSION_DESCRIBE_CLIENT_QUOTAS};
use super::request_header;

/// Match an exact client quota entity name.
pub const CLIENT_QUOTA_MATCH_EXACT: i8 = 0;
/// Match the default client quota entity.
pub const CLIENT_QUOTA_MATCH_DEFAULT: i8 = 1;
/// Match any specified client quota entity name.
pub const CLIENT_QUOTA_MATCH_ANY_SPECIFIED: i8 = 2;

/// One entity component used to filter `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntityFilter {
    /// Kafka quota entity type, for example `user`, `client-id`, or `ip`.
    pub entity_type: String,
    /// Raw Kafka match type.
    pub match_type: i8,
    /// Name to match when `match_type` is exact.
    pub match_value: Option<String>,
}

impl ClientQuotaEntityFilter {
    /// Match an exact quota entity name.
    #[must_use]
    pub fn exact(entity_type: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type: CLIENT_QUOTA_MATCH_EXACT,
            match_value: Some(value.into()),
        }
    }

    /// Match the default quota entity.
    #[must_use]
    pub fn default_entity(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type: CLIENT_QUOTA_MATCH_DEFAULT,
            match_value: None,
        }
    }

    /// Match any specified quota entity name.
    #[must_use]
    pub fn any_specified(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type: CLIENT_QUOTA_MATCH_ANY_SPECIFIED,
            match_value: None,
        }
    }
}

/// Filters for a `DescribeClientQuotas` request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeClientQuotasOptions {
    /// Entity filter components. Empty means all quota entities visible to the broker.
    pub components: Vec<ClientQuotaEntityFilter>,
    /// Whether Kafka should exclude entities with unspecified entity types.
    pub strict: bool,
}

impl DescribeClientQuotasOptions {
    /// Create options that describe all visible client quota entities.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Require strict entity matching.
    #[must_use]
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Add one entity filter component.
    #[must_use]
    pub fn with_component(mut self, component: ClientQuotaEntityFilter) -> Self {
        self.components.push(component);
        self
    }

    /// Replace the entity filter components.
    #[must_use]
    pub fn with_components<I>(mut self, components: I) -> Self
    where
        I: IntoIterator<Item = ClientQuotaEntityFilter>,
    {
        self.components = components.into_iter().collect();
        self
    }
}

/// One entity component used to alter client quotas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntitySpec {
    /// Kafka quota entity type, for example `user`, `client-id`, or `ip`.
    pub entity_type: String,
    /// Entity name, or `None` for Kafka's default entity.
    pub entity_name: Option<String>,
}

impl ClientQuotaEntitySpec {
    /// Create a quota entity with a concrete entity name.
    #[must_use]
    pub fn named(entity_type: impl Into<String>, entity_name: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            entity_name: Some(entity_name.into()),
        }
    }

    /// Create a quota entity that targets Kafka's default entity for this type.
    #[must_use]
    pub fn default_entity(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            entity_name: None,
        }
    }
}

/// One quota operation for `AlterClientQuotas`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlterationOp {
    /// Quota configuration key.
    pub key: String,
    /// Value to set; ignored by Kafka when `remove` is true.
    pub value: f64,
    /// Whether the quota key should be removed.
    pub remove: bool,
}

impl ClientQuotaAlterationOp {
    /// Set a quota value.
    #[must_use]
    pub fn set(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            remove: false,
        }
    }

    /// Remove a quota value.
    #[must_use]
    pub fn remove(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: 0.0,
            remove: true,
        }
    }
}

/// One quota entity alteration entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlteration {
    /// Entity components that identify this quota entry.
    pub entity: Vec<ClientQuotaEntitySpec>,
    /// Quota operations to apply to this entity.
    pub ops: Vec<ClientQuotaAlterationOp>,
}

impl ClientQuotaAlteration {
    /// Create a quota alteration for an entity.
    #[must_use]
    pub fn new<I, J>(entity: I, ops: J) -> Self
    where
        I: IntoIterator<Item = ClientQuotaEntitySpec>,
        J: IntoIterator<Item = ClientQuotaAlterationOp>,
    {
        Self {
            entity: entity.into_iter().collect(),
            ops: ops.into_iter().collect(),
        }
    }
}

/// Options for an `AlterClientQuotas` request.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterClientQuotasOptions {
    /// Quota entries to alter.
    pub entries: Vec<ClientQuotaAlteration>,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl AlterClientQuotasOptions {
    /// Create options with the supplied quota alterations.
    #[must_use]
    pub fn new<I>(entries: I) -> Self
    where
        I: IntoIterator<Item = ClientQuotaAlteration>,
    {
        Self {
            entries: entries.into_iter().collect(),
            validate_only: false,
        }
    }

    /// Validate the request without applying it.
    #[must_use]
    pub fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }
}

/// One entity component returned by `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntity {
    /// Kafka quota entity type.
    pub entity_type: String,
    /// Entity name, or `None` for Kafka's default entity.
    pub entity_name: Option<String>,
}

/// One quota key/value returned by `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaValue {
    /// Quota configuration key.
    pub key: String,
    /// Quota configuration value.
    pub value: f64,
}

/// One quota entity entry returned by `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaEntry {
    /// Entity components that identify this quota entry.
    pub entity: Vec<ClientQuotaEntity>,
    /// Quota values configured for the entity.
    pub values: Vec<ClientQuotaValue>,
}

/// Parsed response from a `DescribeClientQuotas` request.
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Quota entries returned by the broker.
    pub entries: Option<Vec<ClientQuotaEntry>>,
}

/// One quota entity result returned by `AlterClientQuotas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterClientQuotaEntryResult {
    /// Per-entry broker error code.
    pub error_code: i16,
    /// Optional per-entry broker error message.
    pub error_message: Option<String>,
    /// Entity components that identify this quota entry.
    pub entity: Vec<ClientQuotaEntity>,
}

/// Parsed response from an `AlterClientQuotas` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterClientQuotasResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-entity quota alteration results returned by the broker.
    pub entries: Vec<AlterClientQuotaEntryResult>,
}

pub fn build_describe_client_quotas_request(
    correlation_id: i32,
    client_id: &str,
    options: &DescribeClientQuotasOptions,
) -> (RequestHeader, DescribeClientQuotasRequest) {
    use kafka_protocol::messages::describe_client_quotas_request::ComponentData;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeClientQuotas,
        API_VERSION_DESCRIBE_CLIENT_QUOTAS,
    );
    let request = DescribeClientQuotasRequest::default()
        .with_components(
            options
                .components
                .iter()
                .map(|component| {
                    ComponentData::default()
                        .with_entity_type(StrBytes::from_string(component.entity_type.clone()))
                        .with_match_type(component.match_type)
                        .with_match(
                            component
                                .match_value
                                .as_ref()
                                .map(|value| StrBytes::from_string(value.clone())),
                        )
                })
                .collect(),
        )
        .with_strict(options.strict);

    (header, request)
}

/// Build an `AlterClientQuotas` request.
pub fn build_alter_client_quotas_request(
    correlation_id: i32,
    client_id: &str,
    options: &AlterClientQuotasOptions,
) -> (RequestHeader, AlterClientQuotasRequest) {
    use kafka_protocol::messages::alter_client_quotas_request::{EntityData, EntryData, OpData};

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterClientQuotas,
        API_VERSION_ALTER_CLIENT_QUOTAS,
    );
    let entries = options
        .entries
        .iter()
        .map(|entry| {
            EntryData::default()
                .with_entity(
                    entry
                        .entity
                        .iter()
                        .map(|entity| {
                            EntityData::default()
                                .with_entity_type(StrBytes::from_string(entity.entity_type.clone()))
                                .with_entity_name(
                                    entity
                                        .entity_name
                                        .as_ref()
                                        .map(|name| StrBytes::from_string(name.clone())),
                                )
                        })
                        .collect(),
                )
                .with_ops(
                    entry
                        .ops
                        .iter()
                        .map(|op| {
                            OpData::default()
                                .with_key(StrBytes::from_string(op.key.clone()))
                                .with_value(op.value)
                                .with_remove(op.remove)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = AlterClientQuotasRequest::default()
        .with_entries(entries)
        .with_validate_only(options.validate_only);

    (header, request)
}

/// Build a `DescribeUserScramCredentials` request.
pub fn convert_describe_client_quotas_response(
    response: DescribeClientQuotasResponse,
) -> DescribeClientQuotasResponseData {
    DescribeClientQuotasResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        entries: response.entries.map(|entries| {
            entries
                .into_iter()
                .map(|entry| ClientQuotaEntry {
                    entity: entry
                        .entity
                        .into_iter()
                        .map(|entity| ClientQuotaEntity {
                            entity_type: entity.entity_type.to_string(),
                            entity_name: entity.entity_name.map(|name| name.to_string()),
                        })
                        .collect(),
                    values: entry
                        .values
                        .into_iter()
                        .map(|value| ClientQuotaValue {
                            key: value.key.to_string(),
                            value: value.value,
                        })
                        .collect(),
                })
                .collect()
        }),
    }
}

/// Convert a generated `AlterClientQuotasResponse` into the crate's public shape.
pub fn convert_alter_client_quotas_response(
    response: AlterClientQuotasResponse,
) -> AlterClientQuotasResponseData {
    AlterClientQuotasResponseData {
        throttle_time_ms: response.throttle_time_ms,
        entries: response
            .entries
            .into_iter()
            .map(|entry| AlterClientQuotaEntryResult {
                error_code: entry.error_code,
                error_message: entry.error_message.map(|message| message.to_string()),
                entity: entry
                    .entity
                    .into_iter()
                    .map(|entity| ClientQuotaEntity {
                        entity_type: entity.entity_type.to_string(),
                        entity_name: entity.entity_name.map(|name| name.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::alter_client_quotas_response::{
        EntityData as KpAlterClientQuotaEntity, EntryData as KpAlterClientQuotaEntry,
    };
    use kafka_protocol::messages::describe_client_quotas_response::{
        EntityData as KpClientQuotaEntity, EntryData as KpClientQuotaEntry,
        ValueData as KpClientQuotaValue,
    };
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn describe_client_quotas_request_accepts_entity_filters() {
        let options = DescribeClientQuotasOptions::new()
            .with_component(ClientQuotaEntityFilter::exact("user", "alice"))
            .with_component(ClientQuotaEntityFilter::default_entity("client-id"))
            .with_component(ClientQuotaEntityFilter::any_specified("ip"))
            .strict();
        let (header, request) = build_describe_client_quotas_request(23, "client-r", &options);

        assert_eq!(header.request_api_key, ApiKey::DescribeClientQuotas as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_CLIENT_QUOTAS
        );
        assert!(request.strict);
        assert_eq!(request.components.len(), 3);
        assert_eq!(request.components[0].entity_type.to_string(), "user");
        assert_eq!(request.components[0].match_type, CLIENT_QUOTA_MATCH_EXACT);
        assert_eq!(
            request.components[0]
                ._match
                .as_ref()
                .map(ToString::to_string),
            Some("alice".to_owned())
        );
        assert_eq!(request.components[1].match_type, CLIENT_QUOTA_MATCH_DEFAULT);
        assert!(request.components[1]._match.is_none());
        assert_eq!(
            request.components[2].match_type,
            CLIENT_QUOTA_MATCH_ANY_SPECIFIED
        );
        assert!(request.components[2]._match.is_none());
    }

    #[test]
    fn alter_client_quotas_request_preserves_entities_and_ops() {
        let options = AlterClientQuotasOptions::new([ClientQuotaAlteration::new(
            [
                ClientQuotaEntitySpec::named("user", "alice"),
                ClientQuotaEntitySpec::default_entity("client-id"),
            ],
            [
                ClientQuotaAlterationOp::set("producer_byte_rate", 1024.5),
                ClientQuotaAlterationOp::remove("consumer_byte_rate"),
            ],
        )])
        .with_validate_only(true);
        let (header, request) = build_alter_client_quotas_request(24, "client-r", &options);

        assert_eq!(header.request_api_key, ApiKey::AlterClientQuotas as i16);
        assert_eq!(header.request_api_version, API_VERSION_ALTER_CLIENT_QUOTAS);
        assert!(request.validate_only);
        let entry = &request.entries[0];
        assert_eq!(entry.entity[0].entity_type.to_string(), "user");
        assert_eq!(
            entry.entity[0]
                .entity_name
                .as_ref()
                .map(ToString::to_string),
            Some("alice".to_owned())
        );
        assert_eq!(entry.entity[1].entity_type.to_string(), "client-id");
        assert!(entry.entity[1].entity_name.is_none());
        assert_eq!(entry.ops[0].key.to_string(), "producer_byte_rate");
        assert!((entry.ops[0].value - 1024.5).abs() < f64::EPSILON);
        assert!(!entry.ops[0].remove);
        assert_eq!(entry.ops[1].key.to_string(), "consumer_byte_rate");
        assert!(entry.ops[1].remove);
    }
    #[test]
    fn convert_describe_client_quotas_response_preserves_entities_and_values() {
        let response = DescribeClientQuotasResponse::default()
            .with_throttle_time_ms(19)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_entries(Some(vec![
                KpClientQuotaEntry::default()
                    .with_entity(vec![
                        KpClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("user"))
                            .with_entity_name(Some(StrBytes::from_static_str("alice"))),
                        KpClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("client-id"))
                            .with_entity_name(None),
                    ])
                    .with_values(vec![
                        KpClientQuotaValue::default()
                            .with_key(StrBytes::from_static_str("producer_byte_rate"))
                            .with_value(1024.5),
                    ]),
            ]));

        let converted = convert_describe_client_quotas_response(response);

        assert_eq!(converted.throttle_time_ms, 19);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        let entry = &converted.entries.as_ref().unwrap()[0];
        assert_eq!(entry.entity[0].entity_type, "user");
        assert_eq!(entry.entity[0].entity_name, Some("alice".to_owned()));
        assert_eq!(entry.entity[1].entity_type, "client-id");
        assert!(entry.entity[1].entity_name.is_none());
        assert_eq!(entry.values[0].key, "producer_byte_rate");
        assert!((entry.values[0].value - 1024.5).abs() < f64::EPSILON);
    }

    #[test]
    fn convert_alter_client_quotas_response_preserves_entry_errors() {
        let response = AlterClientQuotasResponse::default()
            .with_throttle_time_ms(20)
            .with_entries(vec![
                KpAlterClientQuotaEntry::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_entity(vec![
                        KpAlterClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("user"))
                            .with_entity_name(Some(StrBytes::from_static_str("alice"))),
                        KpAlterClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("client-id"))
                            .with_entity_name(None),
                    ]),
            ]);

        let converted = convert_alter_client_quotas_response(response);

        assert_eq!(converted.throttle_time_ms, 20);
        assert_eq!(converted.entries[0].error_code, 0);
        assert_eq!(converted.entries[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.entries[0].entity[0].entity_type, "user");
        assert_eq!(
            converted.entries[0].entity[0].entity_name,
            Some("alice".to_owned())
        );
        assert_eq!(converted.entries[0].entity[1].entity_type, "client-id");
        assert!(converted.entries[0].entity[1].entity_name.is_none());
    }
}
