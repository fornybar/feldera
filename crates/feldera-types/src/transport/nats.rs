use std::path::PathBuf;
use time::OffsetDateTime;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

// TODO How does the user choose? Think about what "UI" you would prefer.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum Credentials {
    FromString(String),
    #[schema(value_type = String, example = "/path/to/credentials.json")]
    FromFile(PathBuf),
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct UserAndPassword {
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub struct Auth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nkey: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_and_password: Option<UserAndPassword>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ConnectOptions {
    pub server_url: String,
    #[serde(default, skip_serializing_if = "is_default")]
    pub auth: Auth,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub enum ReplayPolicy {
    #[default]
    Instant,
    Original,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub enum DeliverPolicy {
    All,
    Last,
    New,
    ByStartSequence {
        start_sequence: u64,
    },
    ByStartTime {
        #[schema(value_type = String, format = "date-time", example = "2023-01-15T09:30:00Z")]
        start_time: OffsetDateTime,
    },
    LastPerSubject,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct ConsumerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subjects: Vec<String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub replay_policy: ReplayPolicy,
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    pub deliver_policy: DeliverPolicy,
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_waiting: i64,
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_expires: Option<std::time::Duration>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct NatsInputConfig {
    pub connection_config: ConnectOptions,
    pub stream_name: String,
    pub consumer_config: ConsumerConfig,

    /// Whether to include NATS message subject in the record metadata.
    ///
    /// When `true`, the subject is available via the `CONNECTOR_METADATA()` function
    /// as `nats_subject`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_subject: Option<bool>,

    /// Whether to include NATS message headers in the record metadata.
    ///
    /// When `true`, headers are available via the `CONNECTOR_METADATA()` function
    /// as `nats_headers` (a map of header names to binary values).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_headers: Option<bool>,

    /// Whether to include the JetStream stream name in the record metadata.
    ///
    /// When `true`, the stream name is available via the `CONNECTOR_METADATA()` function
    /// as `nats_stream`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_stream: Option<bool>,

    /// Whether to include the JetStream consumer name in the record metadata.
    ///
    /// When `true`, the consumer name is available via the `CONNECTOR_METADATA()` function
    /// as `nats_consumer`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_consumer: Option<bool>,

    /// Whether to include the JetStream stream sequence number in the record metadata.
    ///
    /// When `true`, the stream sequence is available via the `CONNECTOR_METADATA()` function
    /// as `nats_stream_sequence`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_stream_sequence: Option<bool>,

    /// Whether to include the JetStream consumer sequence number in the record metadata.
    ///
    /// When `true`, the consumer sequence is available via the `CONNECTOR_METADATA()` function
    /// as `nats_consumer_sequence`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_consumer_sequence: Option<bool>,

    /// Whether to include the number of delivery attempts in the record metadata.
    ///
    /// When `true`, the delivery count is available via the `CONNECTOR_METADATA()` function
    /// as `nats_delivered`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_delivered: Option<bool>,

    /// Whether to include the number of pending messages in the record metadata.
    ///
    /// When `true`, the pending count is available via the `CONNECTOR_METADATA()` function
    /// as `nats_pending`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_pending: Option<bool>,

    /// Whether to include the message publish timestamp in the record metadata.
    ///
    /// When `true`, the publish timestamp is available via the `CONNECTOR_METADATA()` function
    /// as `nats_published`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_published: Option<bool>,
}

impl NatsInputConfig {
    /// Returns true if any metadata field is requested.
    pub fn metadata_requested(&self) -> bool {
        self.include_subject == Some(true)
            || self.include_headers == Some(true)
            || self.include_stream == Some(true)
            || self.include_consumer == Some(true)
            || self.include_stream_sequence == Some(true)
            || self.include_consumer_sequence == Some(true)
            || self.include_delivered == Some(true)
            || self.include_pending == Some(true)
            || self.include_published == Some(true)
    }
}
