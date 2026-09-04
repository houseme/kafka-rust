#![allow(clippy::wildcard_imports)]
//! Configuration administration helpers.

use kafka_protocol::messages::{
    AlterConfigsRequest, AlterConfigsResponse, ApiKey, DescribeConfigsRequest,
    DescribeConfigsResponse, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    ListConfigResourcesRequest, ListConfigResourcesResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_ALTER_CONFIGS, API_VERSION_DESCRIBE_CONFIGS, API_VERSION_INCREMENTAL_ALTER_CONFIGS,
    API_VERSION_LIST_CONFIG_RESOURCES,
};
use super::request_header;

/// Topic config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_TOPIC: i8 = 2;
/// Broker config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER: i8 = 4;
/// Broker logger config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;

/// Set a config key to a value in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_SET: i8 = 0;
/// Delete a config key in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_DELETE: i8 = 1;
/// Append a value to a list config in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_APPEND: i8 = 2;
/// Subtract a value from a list config in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_SUBTRACT: i8 = 3;

/// A resource whose configs should be described.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name, such as a topic name or broker ID.
    pub resource_name: String,
    /// Configuration keys to fetch, or `None` to fetch all keys.
    pub configuration_keys: Option<Vec<String>>,
}

impl ConfigResource {
    /// Create a config resource with a raw Kafka resource type.
    #[must_use]
    pub fn new(resource_type: i8, resource_name: impl Into<String>) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            configuration_keys: None,
        }
    }

    /// Create a topic config resource.
    #[must_use]
    pub fn topic(name: impl Into<String>) -> Self {
        Self::new(CONFIG_RESOURCE_TYPE_TOPIC, name)
    }

    /// Create a broker config resource.
    #[must_use]
    pub fn broker(id: impl Into<String>) -> Self {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER, id)
    }

    /// Create a broker logger config resource.
    #[must_use]
    pub fn broker_logger(id: impl Into<String>) -> Self {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER_LOGGER, id)
    }

    /// Restrict the request to the supplied configuration keys.
    #[must_use]
    pub fn with_configuration_keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.configuration_keys = Some(keys.into_iter().map(Into::into).collect());
        self
    }
}

/// One config operation for `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfig {
    /// Configuration key name.
    pub name: String,
    /// Raw Kafka config operation code.
    pub operation: i8,
    /// Value used by set/append/subtract operations, or `None` for delete.
    pub value: Option<String>,
}

impl IncrementalAlterConfig {
    /// Create a config operation with a raw Kafka operation code.
    #[must_use]
    pub fn new(name: impl Into<String>, operation: i8, value: Option<String>) -> Self {
        Self {
            name: name.into(),
            operation,
            value,
        }
    }

    /// Set a config key to a value.
    #[must_use]
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_SET, Some(value.into()))
    }

    /// Delete a config key.
    #[must_use]
    pub fn delete(name: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_DELETE, None)
    }

    /// Append a value to a list config key.
    #[must_use]
    pub fn append(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_APPEND, Some(value.into()))
    }

    /// Subtract a value from a list config key.
    #[must_use]
    pub fn subtract(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_SUBTRACT, Some(value.into()))
    }
}

/// One resource updated by `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name, such as a topic name or broker ID.
    pub resource_name: String,
    /// Config operations for this resource.
    pub configs: Vec<IncrementalAlterConfig>,
}

impl IncrementalAlterConfigsResource {
    /// Create a config mutation resource with a raw Kafka resource type.
    #[must_use]
    pub fn new<I>(resource_type: i8, resource_name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            configs: configs.into_iter().collect(),
        }
    }

    /// Create a topic config mutation resource.
    #[must_use]
    pub fn topic<I>(name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_TOPIC, name, configs)
    }

    /// Create a broker config mutation resource.
    #[must_use]
    pub fn broker<I>(id: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER, id, configs)
    }

    /// Create a broker logger config mutation resource.
    #[must_use]
    pub fn broker_logger<I>(id: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER_LOGGER, id, configs)
    }
}

/// Options for an `IncrementalAlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsOptions {
    /// Resources to mutate.
    pub resources: Vec<IncrementalAlterConfigsResource>,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl IncrementalAlterConfigsOptions {
    /// Create options with the supplied resources.
    #[must_use]
    pub fn new<I>(resources: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfigsResource>,
    {
        Self {
            resources: resources.into_iter().collect(),
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

/// Per-resource result returned by `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResourceResult {
    /// Per-resource broker error code.
    pub error_code: i16,
    /// Optional per-resource broker error message.
    pub error_message: Option<String>,
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
}

/// Parsed response from an `IncrementalAlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-resource config mutation results.
    pub responses: Vec<IncrementalAlterConfigsResourceResult>,
}

/// One configurable resource returned by `ListConfigResources`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedConfigResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
}

/// Parsed response from a `ListConfigResources` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Config resources returned by the broker.
    pub resources: Vec<ListedConfigResource>,
}

/// A config synonym returned by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSynonym {
    /// Synonym name.
    pub name: String,
    /// Synonym value, omitted by Kafka for sensitive values.
    pub value: Option<String>,
    /// Raw Kafka config source code for the synonym.
    pub source: i8,
}

/// A config entry returned by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// Config key name.
    pub name: String,
    /// Config value, omitted by Kafka for sensitive values.
    pub value: Option<String>,
    /// Whether the config is read-only.
    pub read_only: bool,
    /// Raw Kafka config source code.
    pub config_source: i8,
    /// Whether the config is sensitive.
    pub is_sensitive: bool,
    /// Config synonyms returned by the broker.
    pub synonyms: Vec<ConfigSynonym>,
    /// Raw Kafka config type code.
    pub config_type: i8,
    /// Optional broker-provided config documentation.
    pub documentation: Option<String>,
}

/// Configs returned for one resource by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    /// Per-resource broker error code.
    pub error_code: i16,
    /// Optional per-resource broker error message.
    pub error_message: Option<String>,
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
    /// Config entries returned for this resource.
    pub configs: Vec<ConfigEntry>,
}

/// Parsed response from a `DescribeConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-resource config results.
    pub results: Vec<DescribeConfigsResult>,
}

/// A config key-value pair for the legacy `AlterConfigs` API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsEntry {
    /// Configuration key name.
    pub name: String,
    /// Configuration value, or `None` to reset to default.
    pub value: Option<String>,
}

impl AlterConfigsEntry {
    /// Create a config entry with a value.
    #[must_use]
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }

    /// Create a config entry that resets to default.
    #[must_use]
    pub fn reset(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
        }
    }
}

/// One resource for the legacy `AlterConfigs` API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name, such as a topic name or broker ID.
    pub resource_name: String,
    /// Config entries to apply.
    pub configs: Vec<AlterConfigsEntry>,
}

impl AlterConfigsResource {
    /// Create a config resource with a raw Kafka resource type.
    #[must_use]
    pub fn new<I>(resource_type: i8, resource_name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsEntry>,
    {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            configs: configs.into_iter().collect(),
        }
    }

    /// Create a topic config resource.
    #[must_use]
    pub fn topic<I>(name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsEntry>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_TOPIC, name, configs)
    }

    /// Create a broker config resource.
    #[must_use]
    pub fn broker<I>(id: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsEntry>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER, id, configs)
    }
}

/// Options for a legacy `AlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsOptions {
    /// Resources to update.
    pub resources: Vec<AlterConfigsResource>,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl AlterConfigsOptions {
    /// Create options with the supplied resources.
    #[must_use]
    pub fn new<I>(resources: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsResource>,
    {
        Self {
            resources: resources.into_iter().collect(),
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

/// Per-resource result returned by the legacy `AlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResourceResult {
    /// Per-resource broker error code.
    pub error_code: i16,
    /// Optional per-resource broker error message.
    pub error_message: Option<String>,
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
}

/// Parsed response from a legacy `AlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-resource config mutation results.
    pub responses: Vec<AlterConfigsResourceResult>,
}

pub fn build_describe_configs_request(
    correlation_id: i32,
    client_id: &str,
    resources: &[ConfigResource],
    include_synonyms: bool,
    include_documentation: bool,
) -> (RequestHeader, DescribeConfigsRequest) {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeConfigs,
        API_VERSION_DESCRIBE_CONFIGS,
    );
    let resources = resources
        .iter()
        .map(|resource| {
            DescribeConfigsResource::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(StrBytes::from_string(resource.resource_name.clone()))
                .with_configuration_keys(resource.configuration_keys.as_ref().map(|keys| {
                    keys.iter()
                        .map(|key| StrBytes::from_string(key.clone()))
                        .collect()
                }))
        })
        .collect();
    let request = DescribeConfigsRequest::default()
        .with_resources(resources)
        .with_include_synonyms(include_synonyms)
        .with_include_documentation(include_documentation);

    (header, request)
}

/// Build an `IncrementalAlterConfigs` request.
pub fn build_incremental_alter_configs_request(
    correlation_id: i32,
    client_id: &str,
    options: &IncrementalAlterConfigsOptions,
) -> (RequestHeader, IncrementalAlterConfigsRequest) {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::IncrementalAlterConfigs,
        API_VERSION_INCREMENTAL_ALTER_CONFIGS,
    );
    let resources = options
        .resources
        .iter()
        .map(|resource| {
            AlterConfigsResource::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(StrBytes::from_string(resource.resource_name.clone()))
                .with_configs(
                    resource
                        .configs
                        .iter()
                        .map(|config| {
                            AlterableConfig::default()
                                .with_name(StrBytes::from_string(config.name.clone()))
                                .with_config_operation(config.operation)
                                .with_value(
                                    config
                                        .value
                                        .as_ref()
                                        .map(|value| StrBytes::from_string(value.clone())),
                                )
                        })
                        .collect(),
                )
        })
        .collect();
    let request = IncrementalAlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(options.validate_only);

    (header, request)
}

/// Build a legacy `AlterConfigs` request.
pub fn build_alter_configs_request(
    correlation_id: i32,
    client_id: &str,
    options: &AlterConfigsOptions,
) -> (RequestHeader, AlterConfigsRequest) {
    use kafka_protocol::messages::alter_configs_request::AlterConfigsResource as KpAlterConfigsResource;
    use kafka_protocol::messages::alter_configs_request::AlterableConfig as KpAlterableConfig;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterConfigs,
        API_VERSION_ALTER_CONFIGS,
    );
    let resources: Vec<KpAlterConfigsResource> = options
        .resources
        .iter()
        .map(|resource| {
            KpAlterConfigsResource::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(StrBytes::from_string(resource.resource_name.clone()))
                .with_configs(
                    resource
                        .configs
                        .iter()
                        .map(|config| {
                            KpAlterableConfig::default()
                                .with_name(StrBytes::from_string(config.name.clone()))
                                .with_value(
                                    config
                                        .value
                                        .as_deref()
                                        .map(|v| StrBytes::from_string(v.to_owned())),
                                )
                        })
                        .collect(),
                )
        })
        .collect();
    let request = AlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(options.validate_only);

    (header, request)
}

/// Build an `AlterReplicaLogDirs` request.
pub fn build_list_config_resources_request(
    correlation_id: i32,
    client_id: &str,
    resource_types: &[i8],
) -> (RequestHeader, ListConfigResourcesRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListConfigResources,
        API_VERSION_LIST_CONFIG_RESOURCES,
    );
    let request =
        ListConfigResourcesRequest::default().with_resource_types(resource_types.to_vec());

    (header, request)
}

/// Build a `CreatePartitions` request.
pub fn convert_describe_configs_response(
    response: DescribeConfigsResponse,
) -> DescribeConfigsResponseData {
    DescribeConfigsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| DescribeConfigsResult {
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
                resource_type: result.resource_type,
                resource_name: result.resource_name.to_string(),
                configs: result
                    .configs
                    .into_iter()
                    .map(|config| ConfigEntry {
                        name: config.name.to_string(),
                        value: config.value.map(|value| value.to_string()),
                        read_only: config.read_only,
                        config_source: config.config_source,
                        is_sensitive: config.is_sensitive,
                        synonyms: config
                            .synonyms
                            .into_iter()
                            .map(|synonym| ConfigSynonym {
                                name: synonym.name.to_string(),
                                value: synonym.value.map(|value| value.to_string()),
                                source: synonym.source,
                            })
                            .collect(),
                        config_type: config.config_type,
                        documentation: config
                            .documentation
                            .map(|documentation| documentation.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `IncrementalAlterConfigsResponse` into the crate's public shape.
pub fn convert_incremental_alter_configs_response(
    response: IncrementalAlterConfigsResponse,
) -> IncrementalAlterConfigsResponseData {
    IncrementalAlterConfigsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        responses: response
            .responses
            .into_iter()
            .map(|result| IncrementalAlterConfigsResourceResult {
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
                resource_type: result.resource_type,
                resource_name: result.resource_name.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `AlterConfigsResponse` into the crate's public shape.
pub fn convert_alter_configs_response(response: AlterConfigsResponse) -> AlterConfigsResponseData {
    AlterConfigsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        responses: response
            .responses
            .into_iter()
            .map(|result| AlterConfigsResourceResult {
                error_code: result.error_code,
                error_message: result.error_message.map(|m| m.to_string()),
                resource_type: result.resource_type,
                resource_name: result.resource_name.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `AlterReplicaLogDirsResponse` into the crate's public shape.
pub fn convert_list_config_resources_response(
    response: ListConfigResourcesResponse,
) -> ListConfigResourcesResponseData {
    ListConfigResourcesResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        resources: response
            .config_resources
            .into_iter()
            .map(|resource| ListedConfigResource {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.to_string(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse as KpAlterConfigsResourceResponse;
    use kafka_protocol::messages::describe_configs_response::{
        DescribeConfigsResourceResult as KpDescribeConfigsResourceResult,
        DescribeConfigsResult as KpDescribeConfigsResult,
        DescribeConfigsSynonym as KpDescribeConfigsSynonym,
    };
    use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse as KpIncrementalAlterConfigsResourceResponse;
    use kafka_protocol::messages::list_config_resources_response::ConfigResource as KpListedConfigResource;
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn describe_configs_request_fetches_selected_topic_keys() {
        let resources = [ConfigResource::topic("topic-a")
            .with_configuration_keys(["retention.ms", "cleanup.policy"])];
        let (header, request) =
            build_describe_configs_request(10, "client-e", &resources, true, true);

        assert_eq!(header.request_api_key, ApiKey::DescribeConfigs as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_CONFIGS);
        assert!(request.include_synonyms);
        assert!(request.include_documentation);
        assert_eq!(
            request.resources[0].resource_type,
            CONFIG_RESOURCE_TYPE_TOPIC
        );
        assert_eq!(request.resources[0].resource_name.to_string(), "topic-a");
        assert_eq!(
            request.resources[0]
                .configuration_keys
                .as_ref()
                .map(Vec::len),
            Some(2)
        );
    }

    #[test]
    fn describe_configs_request_fetches_all_broker_keys_when_keys_are_absent() {
        let resources = [ConfigResource::broker("1")];
        let (_, request) = build_describe_configs_request(11, "client-f", &resources, false, false);

        assert_eq!(
            request.resources[0].resource_type,
            CONFIG_RESOURCE_TYPE_BROKER
        );
        assert!(request.resources[0].configuration_keys.is_none());
    }

    #[test]
    fn incremental_alter_configs_request_preserves_operations() {
        let options =
            IncrementalAlterConfigsOptions::new([IncrementalAlterConfigsResource::topic(
                "topic-a",
                [
                    IncrementalAlterConfig::set("retention.ms", "60000"),
                    IncrementalAlterConfig::delete("cleanup.policy"),
                    IncrementalAlterConfig::append("leader.replication.throttled.replicas", "1:2"),
                ],
            )])
            .with_validate_only(true);
        let (header, request) = build_incremental_alter_configs_request(12, "client-f", &options);

        assert_eq!(
            header.request_api_key,
            ApiKey::IncrementalAlterConfigs as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_INCREMENTAL_ALTER_CONFIGS
        );
        assert!(request.validate_only);
        let resource = &request.resources[0];
        assert_eq!(resource.resource_type, CONFIG_RESOURCE_TYPE_TOPIC);
        assert_eq!(resource.resource_name.to_string(), "topic-a");
        assert_eq!(resource.configs[0].name.to_string(), "retention.ms");
        assert_eq!(resource.configs[0].config_operation, CONFIG_OPERATION_SET);
        assert_eq!(
            resource.configs[0].value.as_ref().map(ToString::to_string),
            Some("60000".to_owned())
        );
        assert_eq!(resource.configs[1].name.to_string(), "cleanup.policy");
        assert_eq!(
            resource.configs[1].config_operation,
            CONFIG_OPERATION_DELETE
        );
        assert!(resource.configs[1].value.is_none());
        assert_eq!(
            resource.configs[2].config_operation,
            CONFIG_OPERATION_APPEND
        );
    }
    #[test]
    fn admin_mutation_option_builders_expose_safe_defaults() {
        let config =
            IncrementalAlterConfig::subtract("leader.replication.throttled.replicas", "1:2");
        assert_eq!(config.operation, CONFIG_OPERATION_SUBTRACT);
        assert_eq!(config.value, Some("1:2".to_owned()));

        let config_options =
            IncrementalAlterConfigsOptions::new([IncrementalAlterConfigsResource::broker_logger(
                "1",
                [config],
            )]);
        assert!(!config_options.validate_only);
        assert_eq!(
            config_options.resources[0].resource_type,
            CONFIG_RESOURCE_TYPE_BROKER_LOGGER
        );

        let create_options =
            CreatePartitionsOptions::new([CreatePartitionsTopicSpec::new("topic-a", 4)]);
        assert_eq!(create_options.timeout_ms, 60_000);
        assert!(!create_options.validate_only);
        assert!(create_options.topics[0].assignments.is_none());

        let election_options = ElectLeadersOptions::all_partitions(ELECTION_TYPE_PREFERRED);
        assert_eq!(election_options.timeout_ms, 60_000);
        assert!(election_options.topic_partitions.is_none());

        let reassignment_options =
            AlterPartitionReassignmentsOptions::new([PartitionReassignmentTopicSpec::new(
                "topic-a",
                [PartitionReassignmentSpec::cancel(0)],
            )]);
        assert_eq!(reassignment_options.timeout_ms, 60_000);
        assert!(reassignment_options.allow_replication_factor_change);
        assert!(
            reassignment_options.topics[0].partitions[0]
                .replicas
                .is_none()
        );

        let quota_options = AlterClientQuotasOptions::new([ClientQuotaAlteration::new(
            [ClientQuotaEntitySpec::default_entity("client-id")],
            [ClientQuotaAlterationOp::remove("producer_byte_rate")],
        )]);
        assert!(!quota_options.validate_only);
        assert!(quota_options.entries[0].entity[0].entity_name.is_none());
        assert!(quota_options.entries[0].ops[0].remove);
    }
    #[test]
    fn list_config_resources_request_accepts_resource_type_filters() {
        let (header, request) = build_list_config_resources_request(
            21,
            "client-p",
            &[CONFIG_RESOURCE_TYPE_TOPIC, CONFIG_RESOURCE_TYPE_BROKER],
        );

        assert_eq!(header.request_api_key, ApiKey::ListConfigResources as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_LIST_CONFIG_RESOURCES
        );
        assert_eq!(
            request.resource_types,
            vec![CONFIG_RESOURCE_TYPE_TOPIC, CONFIG_RESOURCE_TYPE_BROKER]
        );
    }
    #[test]
    fn convert_describe_configs_response_preserves_config_metadata() {
        let response = DescribeConfigsResponse::default()
            .with_throttle_time_ms(14)
            .with_results(vec![
                KpDescribeConfigsResult::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a"))
                    .with_configs(vec![
                        KpDescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str("retention.ms"))
                            .with_value(Some(StrBytes::from_static_str("86400000")))
                            .with_read_only(false)
                            .with_config_source(5)
                            .with_is_sensitive(false)
                            .with_synonyms(vec![
                                KpDescribeConfigsSynonym::default()
                                    .with_name(StrBytes::from_static_str("retention.ms"))
                                    .with_value(Some(StrBytes::from_static_str("86400000")))
                                    .with_source(5),
                            ])
                            .with_config_type(2)
                            .with_documentation(Some(StrBytes::from_static_str(
                                "retention window",
                            ))),
                    ]),
            ]);

        let converted = convert_describe_configs_response(response);

        assert_eq!(converted.throttle_time_ms, 14);
        assert_eq!(converted.results[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.results[0].resource_name, "topic-a");
        assert_eq!(converted.results[0].configs[0].name, "retention.ms");
        assert_eq!(
            converted.results[0].configs[0].value,
            Some("86400000".to_owned())
        );
        assert_eq!(converted.results[0].configs[0].synonyms[0].source, 5);
        assert_eq!(
            converted.results[0].configs[0].documentation,
            Some("retention window".to_owned())
        );
    }

    #[test]
    fn convert_incremental_alter_configs_response_preserves_resource_results() {
        let response = IncrementalAlterConfigsResponse::default()
            .with_throttle_time_ms(15)
            .with_responses(vec![
                KpIncrementalAlterConfigsResourceResponse::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a")),
            ]);

        let converted = convert_incremental_alter_configs_response(response);

        assert_eq!(converted.throttle_time_ms, 15);
        assert_eq!(converted.responses[0].error_code, 0);
        assert_eq!(converted.responses[0].error_message, Some("ok".to_owned()));
        assert_eq!(
            converted.responses[0].resource_type,
            CONFIG_RESOURCE_TYPE_TOPIC
        );
        assert_eq!(converted.responses[0].resource_name, "topic-a");
    }
    #[test]
    fn convert_list_config_resources_response_preserves_resource_types() {
        let response = ListConfigResourcesResponse::default()
            .with_throttle_time_ms(18)
            .with_error_code(0)
            .with_config_resources(vec![
                KpListedConfigResource::default()
                    .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a")),
                KpListedConfigResource::default()
                    .with_resource_type(CONFIG_RESOURCE_TYPE_BROKER)
                    .with_resource_name(StrBytes::from_static_str("1")),
            ]);

        let converted = convert_list_config_resources_response(response);

        assert_eq!(converted.throttle_time_ms, 18);
        assert_eq!(converted.error_code, 0);
        assert_eq!(
            converted.resources,
            vec![
                ListedConfigResource {
                    resource_type: CONFIG_RESOURCE_TYPE_TOPIC,
                    resource_name: "topic-a".to_owned(),
                },
                ListedConfigResource {
                    resource_type: CONFIG_RESOURCE_TYPE_BROKER,
                    resource_name: "1".to_owned(),
                },
            ]
        );
    }
    #[test]
    fn alter_configs_request_replaces_all_resource_configs() {
        let options = AlterConfigsOptions::new([AlterConfigsResource::topic(
            "topic-a",
            [
                AlterConfigsEntry::new("retention.ms", "86400000"),
                AlterConfigsEntry::reset("cleanup.policy"),
            ],
        )])
        .with_validate_only(true);
        let (header, request) = build_alter_configs_request(30, "client-x", &options);

        assert_eq!(header.request_api_key, ApiKey::AlterConfigs as i16);
        assert_eq!(header.request_api_version, API_VERSION_ALTER_CONFIGS);
        assert!(request.validate_only);
        assert_eq!(
            request.resources[0].resource_type,
            CONFIG_RESOURCE_TYPE_TOPIC
        );
        assert_eq!(request.resources[0].resource_name.to_string(), "topic-a");
        assert_eq!(
            request.resources[0].configs[0].name.to_string(),
            "retention.ms"
        );
        assert_eq!(
            request.resources[0].configs[0]
                .value
                .as_ref()
                .map(ToString::to_string),
            Some("86400000".to_owned())
        );
        assert_eq!(
            request.resources[0].configs[1].name.to_string(),
            "cleanup.policy"
        );
        assert!(request.resources[0].configs[1].value.is_none());
    }
    #[test]
    fn alter_configs_response_maps_all_fields() {
        let response = AlterConfigsResponse::default()
            .with_throttle_time_ms(15)
            .with_responses(vec![
                KpAlterConfigsResourceResponse::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a")),
            ]);

        let converted = convert_alter_configs_response(response);

        assert_eq!(converted.throttle_time_ms, 15);
        assert_eq!(converted.responses[0].error_code, 0);
        assert_eq!(converted.responses[0].error_message, Some("ok".to_owned()));
        assert_eq!(
            converted.responses[0].resource_type,
            CONFIG_RESOURCE_TYPE_TOPIC
        );
        assert_eq!(converted.responses[0].resource_name, "topic-a");
    }
}
