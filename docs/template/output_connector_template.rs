// =============================================================================
// FELDERA OUTPUT CONNECTOR TEMPLATE
// =============================================================================
//
// This template provides a starting point for implementing a new Feldera output
// connector (also called adapter or sink). It includes both non-fault-tolerant
// and fault-tolerant implementations with comprehensive comments.
//
// HOW TO USE THIS TEMPLATE:
// 1. Copy this file to /crates/adapters/src/transport/my_new_output/
// 2. Search and replace "MyNewOutput" with your connector name (e.g., "MongoDB")
// 3. Follow the TODO comments to implement your connector logic
// 4. Register your connector (see Section 5 below)
// 5. Delete the implementation variant you don't need (FT or non-FT)
//
// DOCUMENTATION:
// - Full guide: docs/output-connector-guide.md
// - Core trait: /crates/adapterlib/src/transport.rs (OutputEndpoint)
//
// REFERENCE IMPLEMENTATIONS:
// - Simple (File):      /crates/adapters/src/transport/file.rs
// - Key-Value (Redis):  /crates/adapters/src/transport/redis/output.rs
// - Fault-Tolerant:     /crates/adapters/src/transport/kafka/ft/output.rs
//
// =============================================================================

// =============================================================================
// SECTION 1: IMPORTS
// =============================================================================
//
// These are the standard imports needed for most output connectors.
// Add your external crate imports at the marked location below.
//
// =============================================================================

use anyhow::{anyhow, bail, Result as AnyResult};
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint, Step};
use std::sync::RwLock;
use tracing::{debug, info, info_span, warn};
use tracing::span::EnteredSpan;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// TODO: Add your external crate imports here. Examples:
// use my_system_client::{Client, ClientConfig, Error as MySystemError};
// use tokio::runtime::Runtime;

// =============================================================================
// STUB TYPES (Remove these when implementing your real connector)
// =============================================================================
//
// These stub types allow the template to be conceptually complete.
// Replace them with your actual client types.
//
// =============================================================================

/// Stub client type - replace with your actual client.
/// Example: `redis::Client`, `rdkafka::producer::ThreadedProducer`, etc.
struct MyNewOutputClient {
    connected: bool,
    // TODO: Add your client fields here
}

impl MyNewOutputClient {
    fn new(_connection_string: &str) -> AnyResult<Self> {
        // TODO: Initialize your client here
        Ok(Self { connected: false })
    }

    fn connect(&mut self) -> AnyResult<()> {
        // TODO: Establish connection to your external system
        self.connected = true;
        Ok(())
    }

    fn write(&mut self, _data: &[u8]) -> AnyResult<()> {
        // TODO: Write data to your external system
        Ok(())
    }

    fn set(&mut self, _key: &[u8], _value: &[u8]) -> AnyResult<()> {
        // TODO: Set a key-value pair
        Ok(())
    }

    fn delete(&mut self, _key: &[u8]) -> AnyResult<()> {
        // TODO: Delete a key
        Ok(())
    }

    fn begin_transaction(&mut self) -> AnyResult<()> {
        // TODO: Begin a transaction (for FT connectors)
        Ok(())
    }

    fn commit_transaction(&mut self) -> AnyResult<()> {
        // TODO: Commit the transaction (for FT connectors)
        Ok(())
    }

    fn abort_transaction(&mut self) -> AnyResult<()> {
        // TODO: Abort/rollback the transaction (for FT connectors)
        Ok(())
    }

    fn read_last_committed_step(&self) -> AnyResult<Option<Step>> {
        // TODO: Read the last committed step from your external system.
        // This is crucial for fault-tolerant connectors to detect replays.
        // Return None if no data has been written yet.
        Ok(None)
    }

    fn memory_usage(&self) -> usize {
        // TODO: Return approximate memory usage of your client
        0
    }
}

// =============================================================================
// SECTION 2: CONFIGURATION
// =============================================================================
//
// Define your connector's configuration struct here. This struct will be:
// - Serialized/deserialized from JSON in pipeline configurations
// - Validated when the connector is created
// - Documented in the API schema (via ToSchema)
//
// REGISTRATION: In the real codebase, transport config types live in `feldera-types`.
// In your connector implementation under `crates/adapters`, you should typically:
// - define the config struct in `/crates/feldera-types/src/transport/<name>.rs`
// - export it from `/crates/feldera-types/src/transport/mod.rs`
// - `use feldera_types::transport::<name>::<YourConfig>` here
//
// This template includes a config struct inline so the file is self-contained as a starter,
// but you should move it to `feldera-types` during implementation.
//
// See: docs/output-connector-guide.md#registering-your-connector
//
// =============================================================================

/// Configuration for the MyNewOutput output connector.
///
/// # Example Configuration (JSON)
///
/// ```json
/// {
///     "transport": {
///         "name": "my_new_output",
///         "config": {
///             "connection_string": "my-system://localhost:1234",
///             "timeout_secs": 30,
///             "max_retries": 3,
///             "max_message_size_bytes": 1048576
///         }
///     },
///     "format": {
///         "name": "json",
///         "config": {}
///     }
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct MyNewOutputConfig {
    /// Connection string for the external system.
    /// Format depends on your system (e.g., "host:port", URL, etc.)
    pub connection_string: String,

    /// Connection timeout in seconds.
    /// How long to wait when establishing the initial connection.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u32,

    /// Maximum number of retries for transient failures.
    /// Set to 0 for no retries.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Maximum message size in bytes.
    /// Messages larger than this will cause an error.
    /// Set to None for no limit.
    #[serde(default)]
    pub max_message_size_bytes: Option<usize>,

    // TODO: Add your connector-specific configuration fields here.
    // Examples:
    // - topic: String (for message queues)
    // - table_name: String (for databases)
    // - bucket: String (for object storage)
    // - auth_token: Option<String> (for authenticated systems)
}

// NOTE: `usize` is platform-dependent. If you expose `max_message_size_bytes` via an external API
// and need a stable schema, consider using `u64` instead.

fn default_timeout_secs() -> u32 {
    30
}

fn default_max_retries() -> u32 {
    3
}

impl MyNewOutputConfig {
    /// Validates the configuration and returns an error if invalid.
    pub fn validate(&self) -> AnyResult<()> {
        if self.connection_string.is_empty() {
            bail!("connection_string cannot be empty");
        }
        if self.timeout_secs == 0 {
            bail!("timeout_secs must be greater than 0");
        }
        // TODO: Add your validation logic here
        Ok(())
    }
}

// =============================================================================
// SECTION 3: NON-FAULT-TOLERANT OUTPUT ENDPOINT
// =============================================================================
//
// This is the simpler implementation that doesn't track steps or handle replays.
//
// USE THIS WHEN:
// - Your external system doesn't support transactions
// - You can tolerate duplicate data on pipeline restart
// - You're building a development/testing connector
// - Simplicity is more important than exactly-once semantics
//
// LIFECYCLE:
// 1. new() - Create endpoint with config
// 2. connect() - Establish connection, store error callback
// 3. [batch_start()] - Optional, uses default no-op
// 4. push_buffer() / push_key() - Write data (called multiple times)
// 5. [batch_end()] - Optional, uses default no-op
// 6. Repeat 3-5 for each batch
//
// See: docs/output-connector-guide.md#implementing-a-non-fault-tolerant-connector
// Reference: /crates/adapters/src/transport/file.rs
//
// =============================================================================

/// Non-fault-tolerant output endpoint for MyNewOutput.
///
/// This endpoint writes data directly without transaction support.
/// It does not handle replays - duplicate data may be written on pipeline restart.
pub struct MyNewOutputEndpoint {
    /// Configuration for this endpoint.
    config: MyNewOutputConfig,

    /// Client connection to the external system.
    client: MyNewOutputClient,

    /// Callback for reporting asynchronous errors.
    /// Stored during connect() for later use.
    async_error_callback: Option<AsyncErrorCallback>,
}

impl MyNewOutputEndpoint {
    /// Creates a new non-fault-tolerant MyNewOutput endpoint.
    ///
    /// This validates the configuration and initializes the client,
    /// but does not establish the connection yet (that happens in connect()).
    pub fn new(config: MyNewOutputConfig) -> AnyResult<Self> {
        // Validate configuration early to fail fast
        config.validate()?;

        // Initialize the client (but don't connect yet)
        let client = MyNewOutputClient::new(&config.connection_string)?;

        Ok(Self {
            config,
            client,
            async_error_callback: None,
        })
    }

    /// Creates a tracing span for this endpoint.
    /// Use this to wrap operations for structured logging.
    fn span(&self) -> EnteredSpan {
        // Avoid logging secrets (connection strings often embed credentials).
        info_span!("my_new_output", ft = false).entered()
    }

    /// Reports an asynchronous error using the stored callback.
    ///
    /// Call this when an error occurs outside of a trait method
    /// (e.g., in a background thread, callback, or async handler).
    ///
    /// # Arguments
    /// - `fatal`: If true, the endpoint cannot recover and the pipeline will stop
    /// - `error`: The error to report
    #[allow(dead_code)]
    fn report_async_error(&self, fatal: bool, error: anyhow::Error) {
        if let Some(ref callback) = self.async_error_callback {
            callback(fatal, error, Some("my_new_output_error"));
        } else {
            // Callback not set yet, just log
            if fatal {
                tracing::error!("Fatal async error (no callback): {error}");
            } else {
                tracing::warn!("Async error (no callback): {error}");
            }
        }
    }
}

impl OutputEndpoint for MyNewOutputEndpoint {
    /// Establishes the connection to the external system.
    ///
    /// This is called once after the endpoint is created.
    /// Store the async_error_callback for reporting errors that occur
    /// outside of trait methods (e.g., in callbacks or background threads).
    ///
    /// See: docs/output-connector-guide.md#the-outputendpoint-trait
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let _guard = self.span();
        info!("Connecting to MyNewOutput system");

        // Store the callback for later use
        self.async_error_callback = Some(async_error_callback);

        // TODO: Establish connection to your external system
        self.client.connect().map_err(|e| {
            anyhow!("Failed to connect to MyNewOutput: {e}")
        })?;

        info!("Successfully connected to MyNewOutput system");
        Ok(())
    }

    /// Returns the maximum buffer size this transport can handle.
    ///
    /// The encoder will not produce buffers larger than this value.
    /// Return usize::MAX if there's no practical limit.
    ///
    /// Common limits:
    /// - Kafka: ~1MB (configurable via message.max.bytes)
    /// - HTTP: Often 10MB-100MB depending on server
    /// - Files: usize::MAX (no limit)
    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_message_size_bytes.unwrap_or(usize::MAX)
    }

    // NOTE: batch_start() uses the default implementation (no-op) for non-FT connectors.
    // Uncomment and implement if you need to do something at batch boundaries.
    //
    // fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
    //     Ok(())
    // }

    /// Writes a buffer of encoded data to the external system.
    ///
    /// This is the main method for connectors that work with plain byte buffers.
    /// It may be called multiple times per batch.
    ///
    /// # Arguments
    /// - `buffer`: The encoded data to write (format depends on the encoder)
    ///
    /// # Errors
    /// Return an error if the write fails. The pipeline will retry or stop
    /// depending on the error type and configuration.
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();
        debug!("Writing {} bytes to MyNewOutput", buffer.len());

        // TODO: Implement your write logic here
        self.client.write(buffer).map_err(|e| {
            anyhow!("Failed to write to MyNewOutput: {e}")
        })?;

        Ok(())
    }

    /// Writes a key-value pair to the external system.
    ///
    /// This method is used by formats that produce key-value pairs (e.g., Debezium).
    /// Implement this if your system natively supports key-value storage.
    ///
    /// # Arguments
    /// - `key`: The key (None means no key, which may be an error for some systems)
    /// - `val`: The value (None typically means "delete this key")
    /// - `headers`: Optional headers (mainly used by Kafka)
    ///
    /// # Key-Value Semantics
    /// - `key=Some, val=Some` -> SET/UPSERT the key-value pair
    /// - `key=Some, val=None` -> DELETE the key
    /// - `key=None, val=Some` -> System-dependent (often an error)
    /// - `key=None, val=None` -> Usually an error
    ///
    /// See: docs/output-connector-guide.md#key-value-connectors
    /// Reference: /crates/adapters/src/transport/redis/output.rs
    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = self.span();

        // Most key-value systems require a key
        let key = key.ok_or_else(|| {
            anyhow!("MyNewOutput requires a key for each record")
        })?;

        match val {
            Some(value) => {
                // SET operation: key-value pair provided
                debug!("Setting key ({} bytes) = value ({} bytes)", key.len(), value.len());
                self.client.set(key, value).map_err(|e| {
                    anyhow!("Failed to set key in MyNewOutput: {e}")
                })?;
            }
            None => {
                // DELETE operation: key provided but no value
                debug!("Deleting key ({} bytes)", key.len());
                self.client.delete(key).map_err(|e| {
                    anyhow!("Failed to delete key in MyNewOutput: {e}")
                })?;
            }
        }

        Ok(())
    }

    // NOTE: batch_end() uses the default implementation (no-op) for non-FT connectors.
    // Uncomment and implement if you need to flush or sync at batch boundaries.
    //
    // fn batch_end(&mut self) -> AnyResult<()> {
    //     // Example: flush any buffered data
    //     // self.client.flush()?;
    //     Ok(())
    // }

    /// Returns whether this endpoint is fault-tolerant.
    ///
    /// Non-fault-tolerant endpoints return false.
    /// This tells Feldera not to expect replay handling.
    fn is_fault_tolerant(&self) -> bool {
        false
    }

    // NOTE: memory() uses the default implementation (returns 0) for simple connectors.
    // Implement this if your connector uses significant memory that should be tracked.
    //
    // fn memory(&self) -> usize {
    //     self.client.memory_usage()
    // }
}

// =============================================================================
// SECTION 4: FAULT-TOLERANT OUTPUT ENDPOINT
// =============================================================================
//
// This implementation tracks step numbers and handles replays for exactly-once
// semantics. It requires your external system to support transactions.
//
// USE THIS WHEN:
// - Your external system supports transactions (begin/commit/abort)
// - You need exactly-once delivery semantics
// - You can read back the last committed step number
//
// KEY REQUIREMENTS:
// 1. Track the next expected step number (next_step)
// 2. On batch_start(step): if step < next_step, this is a replay - discard it
// 3. On batch_start(step): if step >= next_step, begin a transaction
// 4. On push_buffer/push_key: only write if not a replay
// 5. On batch_end: if not a replay, commit the transaction
// 6. Data must NOT be visible to readers until batch_end commits
//
// REPLAY DETECTION FLOW:
//
//   connect() reads last_step from external system
//        │
//        └──► next_step = last_step + 1 (e.g., next_step = 6)
//        │
//        ▼
//   batch_start(step=3)     ◄── Replay! step(3) < next_step(6)
//        │
//        └──► is_replay = true, skip transaction
//        │
//        ▼
//   push_buffer(data)
//        │
//        └──► Data discarded (is_replay = true)
//        │
//        ▼
//   batch_end()
//        │
//        └──► Nothing to commit (is_replay = true)
//        │
//        ▼
//   batch_start(step=6)     ◄── New data! step(6) >= next_step(6)
//        │
//        └──► is_replay = false, begin transaction
//        │
//        ▼
//   push_buffer(data)
//        │
//        └──► Write data to transaction
//        │
//        ▼
//   batch_end()
//        │
//        └──► Commit transaction, next_step = 7
//
// See: docs/output-connector-guide.md#implementing-a-fault-tolerant-connector
// Reference: /crates/adapters/src/transport/kafka/ft/output.rs
//
// =============================================================================

/// State machine for the fault-tolerant endpoint.
///
/// This helps catch programming errors (calling methods in the wrong order).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum FtState {
    /// Just created, connect() not yet called.
    New,

    /// connect() has been called, ready for batches.
    Connected,

    /// batch_start(step) has been called, currently writing data.
    /// The boolean indicates if this is a replay (should discard data).
    BatchOpen { step: Step, is_replay: bool },

    /// batch_end() has been called for the given step.
    BatchClosed(Step),
}

/// Fault-tolerant output endpoint for MyNewOutput.
///
/// This endpoint uses transactions and tracks step numbers to provide
/// exactly-once delivery semantics. On pipeline restart, it detects
/// and discards replayed steps.
pub struct MyNewOutputFtEndpoint {
    /// Configuration for this endpoint.
    config: MyNewOutputConfig,

    /// Client connection to the external system.
    client: MyNewOutputClient,

    /// Current state of the endpoint.
    state: FtState,

    /// The next step number we expect to write.
    /// Any step < next_step has already been written and should be discarded.
    next_step: Step,

    /// Callback for reporting asynchronous errors.
    async_error_callback: RwLock<Option<AsyncErrorCallback>>,
}

impl MyNewOutputFtEndpoint {
    /// Creates a new fault-tolerant MyNewOutput endpoint.
    pub fn new(config: MyNewOutputConfig) -> AnyResult<Self> {
        config.validate()?;

        let client = MyNewOutputClient::new(&config.connection_string)?;

        Ok(Self {
            config,
            client,
            state: FtState::New,
            next_step: 0,
            async_error_callback: RwLock::new(None),
        })
    }

    /// Creates a tracing span for this endpoint.
    fn span(&self) -> EnteredSpan {
        // Avoid logging secrets (connection strings often embed credentials).
        info_span!("my_new_output", ft = true).entered()
    }

    /// Reports an asynchronous error using the stored callback.
    #[allow(dead_code)]
    fn report_async_error(&self, fatal: bool, error: anyhow::Error) {
        if let Some(ref callback) = *self.async_error_callback.read().unwrap() {
            callback(fatal, error, Some("my_new_output_ft_error"));
        }
    }
}

impl OutputEndpoint for MyNewOutputFtEndpoint {
    /// Establishes the connection and determines the next step number.
    ///
    /// For fault-tolerant connectors, this MUST read the last committed step
    /// from the external system to enable replay detection.
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        debug_assert_eq!(self.state, FtState::New);
        let _guard = self.span();
        info!("Connecting to MyNewOutput system (fault-tolerant mode)");

        // Store the callback
        *self.async_error_callback.write().unwrap() = Some(async_error_callback);

        // Connect to external system
        self.client.connect().map_err(|e| {
            anyhow!("Failed to connect to MyNewOutput: {e}")
        })?;

        // CRITICAL: Read the last committed step from the external system.
        // This is how we know which steps have already been written.
        //
        // TODO: Implement read_last_committed_step() in your client.
        // Common patterns:
        // - Store step number as metadata with each batch
        // - Use a separate "checkpoint" table/topic/key
        // - Read the last message and extract step from its key
        self.next_step = match self.client.read_last_committed_step()? {
            Some(last_step) => {
                info!("Resuming from step {} (next_step = {})", last_step, last_step + 1);
                last_step + 1
            }
            None => {
                info!("No previous data found, starting from step 0");
                0
            }
        };

        self.state = FtState::Connected;
        info!("Successfully connected, next_step = {}", self.next_step);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_message_size_bytes.unwrap_or(usize::MAX)
    }

    /// Begins a new batch for the given step.
    ///
    /// This method:
    /// 1. Checks if the step is a replay (step < next_step)
    /// 2. If not a replay, begins a transaction
    /// 3. If a replay, sets is_replay flag to discard subsequent data
    fn batch_start(&mut self, step: Step) -> AnyResult<()> {
        let _guard = self.span();

        // Validate state transitions
        match self.state {
            FtState::New => {
                unreachable!("connect() must be called before batch_start()");
            }
            FtState::Connected => {
                // First batch after connect, OK
            }
            FtState::BatchClosed(closed_step) => {
                if step <= closed_step {
                    unreachable!(
                        "Step numbers must increase: got {step} after {closed_step}"
                    );
                }
            }
            FtState::BatchOpen { .. } => {
                unreachable!("batch_end() must be called before next batch_start()");
            }
        }

        // Check for replay
        let is_replay = step < self.next_step;

        if is_replay {
            // This step has already been written in a previous run.
            // We'll discard all data until batch_end().
            debug!(
                "Replaying step {} (already committed, next_step = {}), will discard",
                step, self.next_step
            );
        } else {
            // This is new data, begin a transaction
            if step > self.next_step {
                // Gap in step numbers - this might indicate a problem
                warn!(
                    "Step gap detected: jumping from {} to {}",
                    self.next_step, step
                );
            }

            debug!("Beginning transaction for step {}", step);

            // TODO: Begin transaction in your external system
            self.client.begin_transaction().map_err(|e| {
                anyhow!("Failed to begin transaction for step {step}: {e}")
            })?;
        }

        self.state = FtState::BatchOpen { step, is_replay };
        Ok(())
    }

    /// Writes data to the external system (if not a replay).
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();

        let FtState::BatchOpen { step, is_replay } = self.state else {
            unreachable!(
                "batch_start() must be called before push_buffer(), state = {:?}",
                self.state
            );
        };

        if is_replay {
            // Silently discard replay data
            debug!("Discarding {} bytes (replay of step {})", buffer.len(), step);
            return Ok(());
        }

        // Write the data
        debug!("Writing {} bytes for step {}", buffer.len(), step);

        // TODO: Write to your external system within the transaction
        self.client.write(buffer).map_err(|e| {
            anyhow!("Failed to write to MyNewOutput for step {step}: {e}")
        })?;

        Ok(())
    }

    /// Writes a key-value pair to the external system (if not a replay).
    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = self.span();

        let FtState::BatchOpen { step, is_replay } = self.state else {
            unreachable!(
                "batch_start() must be called before push_key(), state = {:?}",
                self.state
            );
        };

        if is_replay {
            debug!("Discarding key-value (replay of step {})", step);
            return Ok(());
        }

        let key = key.ok_or_else(|| {
            anyhow!("MyNewOutput requires a key for each record")
        })?;

        match val {
            Some(value) => {
                debug!("Setting key ({} bytes) for step {}", key.len(), step);
                self.client.set(key, value)?;
            }
            None => {
                debug!("Deleting key ({} bytes) for step {}", key.len(), step);
                self.client.delete(key)?;
            }
        }

        Ok(())
    }

    /// Completes the current batch.
    ///
    /// For non-replay batches, this commits the transaction and updates next_step.
    /// For replay batches, this is a no-op.
    fn batch_end(&mut self) -> AnyResult<()> {
        let _guard = self.span();

        let FtState::BatchOpen { step, is_replay } = self.state else {
            unreachable!(
                "batch_start() must be called before batch_end(), state = {:?}",
                self.state
            );
        };

        if is_replay {
            debug!("Batch {} complete (replay, no commit needed)", step);
        } else {
            debug!("Committing transaction for step {}", step);

            // TODO: Commit the transaction in your external system.
            // After this, the data becomes visible to readers.
            self.client.commit_transaction().map_err(|e| {
                anyhow!("Failed to commit transaction for step {step}: {e}")
            })?;

            // Update next_step for future replay detection
            self.next_step = step + 1;
            info!("Step {} committed, next_step = {}", step, self.next_step);
        }

        self.state = FtState::BatchClosed(step);
        Ok(())
    }

    /// Returns true because this is a fault-tolerant endpoint.
    fn is_fault_tolerant(&self) -> bool {
        true
    }

    /// Returns the approximate memory usage of the client.
    ///
    /// Implement this if your connector uses significant memory that should
    /// be tracked for resource management.
    fn memory(&self) -> usize {
        self.client.memory_usage()
    }
}

// =============================================================================
// SECTION 5: REGISTRATION GUIDE
// =============================================================================
//
// Follow these steps to register your connector with Feldera.
//
// STEP 1: Add your config struct to TransportConfig
// -------------------------------------------------
// File: /crates/feldera-types/src/config.rs
//
// Add to the TransportConfig enum:
//
// ```rust
// #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
// #[serde(tag = "name", content = "config", rename_all = "snake_case")]
// pub enum TransportConfig {
//     // ... existing variants ...
//     MyNewOutput(MyNewOutputConfig),  // <-- Add this line
// }
// ```
//
// Also add to the name() method:
//
// ```rust
// impl TransportConfig {
//     pub fn name(&self) -> String {
//         match self {
//             // ... existing matches ...
//             TransportConfig::MyNewOutput(_) => "my_new_output".to_string(),
//         }
//     }
// }
// ```
//
// STEP 2: Register in output_transport_config_to_endpoint()
// ---------------------------------------------------------
// File: /crates/adapters/src/transport.rs
//
// Add to the match statement in output_transport_config_to_endpoint():
//
// ```rust
// pub fn output_transport_config_to_endpoint(
//     config: &TransportConfig,
//     endpoint_name: &str,
//     fault_tolerant: bool,
//     secrets_dir: &Path,
// ) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
//     let config = resolve_secret_references_via_json(secrets_dir, config)?;
//     match config {
//         // ... existing matches ...
//
//         // For connectors with both FT and non-FT variants:
//         TransportConfig::MyNewOutput(config) => match fault_tolerant {
//             false => Ok(Some(Box::new(MyNewOutputEndpoint::new(config)?))),
//             true => Ok(Some(Box::new(MyNewOutputFtEndpoint::new(config)?))),
//         },
//
//         // OR for connectors with only one variant:
//         // TransportConfig::MyNewOutput(config) => {
//         //     Ok(Some(Box::new(MyNewOutputEndpoint::new(config)?)))
//         // }
//
//         _ => Ok(None),
//     }
// }
// ```
//
// STEP 3: (Optional) Add feature flag
// -----------------------------------
// File: /crates/adapters/Cargo.toml
//
// If your connector has external dependencies, add a feature flag:
//
// ```toml
// [features]
// default = ["with-kafka", "with-redis", "with-my-new-output"]
// with-my-new-output = ["my-new-output-client"]
//
// [dependencies]
// my-new-output-client = { version = "1.0", optional = true }
// ```
//
// Then gate your registration code:
//
// ```rust
// #[cfg(feature = "with-my-new-output")]
// TransportConfig::MyNewOutput(config) => { ... }
// ```
//
// =============================================================================

// =============================================================================
// SECTION 6: UNIT TESTS
// =============================================================================
//
// These tests demonstrate how to test your connector.
// Adapt them for your specific implementation.
//
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a test configuration.
    fn test_config() -> MyNewOutputConfig {
        MyNewOutputConfig {
            connection_string: "test://localhost:1234".to_string(),
            timeout_secs: 10,
            max_retries: 3,
            max_message_size_bytes: Some(1024 * 1024),
        }
    }

    /// A no-op error callback for testing.
    fn test_error_callback() -> AsyncErrorCallback {
        Box::new(|_fatal, _error, _tag| {
            // In tests, you might want to panic on errors:
            // panic!("Async error: {_error}");
        })
    }

    // -------------------------------------------------------------------------
    // Configuration Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_config_validation_success() {
        let config = test_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_empty_connection_string() {
        let mut config = test_config();
        config.connection_string = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_timeout() {
        let mut config = test_config();
        config.timeout_secs = 0;
        assert!(config.validate().is_err());
    }

    // -------------------------------------------------------------------------
    // Non-Fault-Tolerant Endpoint Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_non_ft_create_and_connect() {
        let config = test_config();
        let mut endpoint = MyNewOutputEndpoint::new(config).unwrap();
        
        // Connect should succeed
        endpoint.connect(test_error_callback()).unwrap();
    }

    #[test]
    fn test_non_ft_push_buffer() {
        let config = test_config();
        let mut endpoint = MyNewOutputEndpoint::new(config).unwrap();
        endpoint.connect(test_error_callback()).unwrap();

        // Write some data
        endpoint.push_buffer(b"hello world").unwrap();
    }

    #[test]
    fn test_non_ft_push_key_set() {
        let config = test_config();
        let mut endpoint = MyNewOutputEndpoint::new(config).unwrap();
        endpoint.connect(test_error_callback()).unwrap();

        // SET operation
        endpoint.push_key(
            Some(b"my-key"),
            Some(b"my-value"),
            &[],
        ).unwrap();
    }

    #[test]
    fn test_non_ft_push_key_delete() {
        let config = test_config();
        let mut endpoint = MyNewOutputEndpoint::new(config).unwrap();
        endpoint.connect(test_error_callback()).unwrap();

        // DELETE operation (val = None)
        endpoint.push_key(
            Some(b"my-key"),
            None,
            &[],
        ).unwrap();
    }

    #[test]
    fn test_non_ft_push_key_requires_key() {
        let config = test_config();
        let mut endpoint = MyNewOutputEndpoint::new(config).unwrap();
        endpoint.connect(test_error_callback()).unwrap();

        // No key should fail
        let result = endpoint.push_key(None, Some(b"value"), &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_non_ft_is_not_fault_tolerant() {
        let config = test_config();
        let endpoint = MyNewOutputEndpoint::new(config).unwrap();
        assert!(!endpoint.is_fault_tolerant());
    }

    // -------------------------------------------------------------------------
    // Fault-Tolerant Endpoint Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_ft_create_and_connect() {
        let config = test_config();
        let mut endpoint = MyNewOutputFtEndpoint::new(config).unwrap();
        endpoint.connect(test_error_callback()).unwrap();
    }

    #[test]
    fn test_ft_basic_batch_lifecycle() {
        let config = test_config();
        let mut endpoint = MyNewOutputFtEndpoint::new(config).unwrap();
        endpoint.connect(test_error_callback()).unwrap();

        // Complete batch lifecycle
        endpoint.batch_start(0).unwrap();
        endpoint.push_buffer(b"data for step 0").unwrap();
        endpoint.batch_end().unwrap();

        // Next batch
        endpoint.batch_start(1).unwrap();
        endpoint.push_buffer(b"data for step 1").unwrap();
        endpoint.batch_end().unwrap();
    }

    #[test]
    fn test_ft_is_fault_tolerant() {
        let config = test_config();
        let endpoint = MyNewOutputFtEndpoint::new(config).unwrap();
        assert!(endpoint.is_fault_tolerant());
    }

    // -------------------------------------------------------------------------
    // Replay Detection Tests
    // -------------------------------------------------------------------------
    //
    // NOTE: These tests are conceptual. In a real implementation, you'd need
    // to mock the client or use a test database to verify replay detection.
    //
    // The key behavior to test:
    // 1. After writing step N and reconnecting, step N should be discarded
    // 2. Step N+1 should be written normally
    //
    // Example test structure:
    //
    // #[test]
    // fn test_ft_replay_detection() {
    //     // Setup: Create endpoint and write step 5
    //     let mut endpoint1 = create_endpoint_with_test_db();
    //     endpoint1.connect(...);
    //     endpoint1.batch_start(5).unwrap();
    //     endpoint1.push_buffer(b"original data").unwrap();
    //     endpoint1.batch_end().unwrap();
    //     drop(endpoint1);
    //
    //     // Simulate restart: Create new endpoint (reads last step = 5)
    //     let mut endpoint2 = create_endpoint_with_same_test_db();
    //     endpoint2.connect(...);  // Should set next_step = 6
    //
    //     // Replay step 5 - should be discarded
    //     endpoint2.batch_start(5).unwrap();
    //     endpoint2.push_buffer(b"replayed data").unwrap();
    //     endpoint2.batch_end().unwrap();
    //
    //     // Write step 6 - should succeed
    //     endpoint2.batch_start(6).unwrap();
    //     endpoint2.push_buffer(b"new data").unwrap();
    //     endpoint2.batch_end().unwrap();
    //
    //     // Verify: Only "original data" and "new data" exist, not "replayed data"
    //     let all_data = read_all_from_test_db();
    //     assert_eq!(all_data, vec!["original data", "new data"]);
    // }
    //
    // -------------------------------------------------------------------------
}

// =============================================================================
// SECTION 7: INTEGRATION TEST PLACEHOLDER
// =============================================================================
//
// Integration tests run against a real external system (often via Docker).
// These are typically:
// - Marked with #[ignore] so they don't run in normal test suites
// - Run manually or in CI with `cargo test -- --ignored`
// - Configured via environment variables
//
// Example Docker setup (docker-compose.yml):
//
// ```yaml
// version: '3'
// services:
//   my-new-output-system:
//     image: my-new-output:latest
//     ports:
//       - "1234:1234"
// ```
//
// Run tests:
//   docker-compose up -d
//   MY_NEW_OUTPUT_URL=localhost:1234 cargo test -- --ignored
//   docker-compose down
//
// =============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Integration test against a real external system.
    ///
    /// Run with: MY_NEW_OUTPUT_URL=localhost:1234 cargo test -- --ignored
    #[test]
    #[ignore]
    fn test_integration_with_real_system() {
        // Get connection info from environment
        let connection_string = std::env::var("MY_NEW_OUTPUT_URL")
            .expect("Set MY_NEW_OUTPUT_URL to run integration tests");

        let config = MyNewOutputConfig {
            connection_string,
            timeout_secs: 30,
            max_retries: 3,
            max_message_size_bytes: None,
        };

        // Create and connect
        let mut endpoint = MyNewOutputEndpoint::new(config).unwrap();
        endpoint.connect(Box::new(|fatal, error, _tag| {
            if fatal {
                panic!("Fatal error: {error}");
            } else {
                eprintln!("Warning: {error}");
            }
        })).unwrap();

        // Write test data
        endpoint.batch_start(0).unwrap();
        endpoint.push_buffer(b"integration test data").unwrap();
        endpoint.batch_end().unwrap();

        // TODO: Verify the data was written correctly
        // let result = read_from_external_system();
        // assert_eq!(result, "integration test data");
    }

    /// Integration test for fault-tolerant mode.
    #[test]
    #[ignore]
    fn test_ft_integration_with_real_system() {
        let connection_string = std::env::var("MY_NEW_OUTPUT_URL")
            .expect("Set MY_NEW_OUTPUT_URL to run integration tests");

        let config = MyNewOutputConfig {
            connection_string,
            timeout_secs: 30,
            max_retries: 3,
            max_message_size_bytes: None,
        };

        // First run: write steps 0 and 1
        {
            let mut endpoint = MyNewOutputFtEndpoint::new(config.clone()).unwrap();
            endpoint.connect(Box::new(|_, _, _| {})).unwrap();

            endpoint.batch_start(0).unwrap();
            endpoint.push_buffer(b"step 0 data").unwrap();
            endpoint.batch_end().unwrap();

            endpoint.batch_start(1).unwrap();
            endpoint.push_buffer(b"step 1 data").unwrap();
            endpoint.batch_end().unwrap();
        }

        // Second run: simulate restart, replay step 1, write step 2
        {
            let mut endpoint = MyNewOutputFtEndpoint::new(config).unwrap();
            endpoint.connect(Box::new(|_, _, _| {})).unwrap();

            // This should be detected as replay and discarded
            endpoint.batch_start(1).unwrap();
            endpoint.push_buffer(b"step 1 REPLAYED - should be discarded").unwrap();
            endpoint.batch_end().unwrap();

            // This should be written
            endpoint.batch_start(2).unwrap();
            endpoint.push_buffer(b"step 2 data").unwrap();
            endpoint.batch_end().unwrap();
        }

        // TODO: Verify results
        // let all_data = read_all_from_external_system();
        // assert_eq!(all_data, vec!["step 0 data", "step 1 data", "step 2 data"]);
        // Note: "step 1 REPLAYED" should NOT be in the results
    }
}
