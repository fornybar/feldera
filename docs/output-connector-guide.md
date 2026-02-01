# Feldera Output Connector Implementation Guide

This guide explains how to implement output connectors (also called adapters or sinks) for Feldera. Output connectors are responsible for sending processed data from Feldera pipelines to external systems like databases, message queues, files, or APIs.

> **Quick Start**: Use the [output connector template](./template/output_connector_template.rs) to get started quickly. See the [template README](./template/README.md) for step-by-step instructions.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [The OutputEndpoint Trait](#the-outputendpoint-trait)
3. [Implementing a Non-Fault-Tolerant Connector](#implementing-a-non-fault-tolerant-connector)
4. [Implementing a Fault-Tolerant Connector](#implementing-a-fault-tolerant-connector)
5. [Key-Value Connectors](#key-value-connectors)
6. [Integrated Output Endpoints](#integrated-output-endpoints)
7. [Registering Your Connector](#registering-your-connector)
8. [Tracing and Logging](#tracing-and-logging)
9. [Memory Tracking](#memory-tracking)
10. [Advanced Patterns](#advanced-patterns)
11. [Error Handling](#error-handling)
12. [Secrets and Security](#secrets-and-security)
13. [Testing Guidelines](#testing-guidelines)
14. [Connector Feature Matrix](#connector-feature-matrix)

---

## Architecture Overview

### How Output Connectors Fit Into Feldera

```
┌─────────────────────────────────────────────────────────────────┐
│                        Feldera Pipeline                         │
│                                                                 │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────────────┐ │
│  │  Input   │───▶│   DBSP       │───▶│  Output Encoder       │ │
│  │ Adapters │    │  Processing  │    │  (JSON, CSV, etc.)    │ │
│  └──────────┘    └──────────────┘    └───────────┬───────────┘ │
│                                                  │             │
│                                      ┌───────────▼───────────┐ │
│                                      │   OutputEndpoint      │ │
│                                      │   (Your Connector)    │ │
│                                      └───────────┬───────────┘ │
└──────────────────────────────────────────────────┼─────────────┘
                                                   │
                                                   ▼
                                      ┌───────────────────────┐
                                      │   External System     │
                                      │ (Kafka, DB, File...)  │
                                      └───────────────────────┘
```

### Data Flow Lifecycle

Each batch of output data follows this lifecycle:

```
connect() ──▶ batch_start(step) ──▶ push_buffer()/push_key() ──▶ batch_end()
                    │                        │                        │
                    │                        │                        │
              Called once per          Called multiple          Called once per
              batch with step         times with data           batch to commit
              number (for FT)
```

### Types of Output Connectors

1. **Non-Fault-Tolerant**: Simple connectors that don't track steps or support exactly-once delivery. Suitable for development, testing, or systems that can tolerate duplicates.

2. **Fault-Tolerant**: Connectors that track step numbers and can discard duplicate data on replay. Required for exactly-once semantics.

3. **Key-Value Connectors**: Connectors that use `push_key()` instead of `push_buffer()` for systems that natively support key-value pairs (e.g., Redis, Kafka with Debezium).

4. **Integrated Output Endpoints**: Connectors that combine transport AND encoding in one implementation (e.g., PostgreSQL, Delta Lake). These implement both `OutputEndpoint` and `Encoder` traits.

---

## The OutputEndpoint Trait

The core trait for output connectors is `OutputEndpoint`, defined in `/crates/adapterlib/src/transport.rs`:

```rust
pub trait OutputEndpoint: Send {
    /// Finishes establishing the connection to the output endpoint.
    /// 
    /// If the endpoint encounters any errors during output, now or later, it
    /// invokes `async_error_callback` to notify the client about asynchronous
    /// errors.
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()>;

    /// Maximum buffer size that this transport can transmit.
    /// The encoder should not generate buffers exceeding this size.
    fn max_buffer_size_bytes(&self) -> usize;

    /// Notifies the output endpoint that data subsequently written by
    /// `push_buffer` belong to the given `step`.
    /// 
    /// For fault-tolerant endpoints:
    /// 1. If data for the given step has been written before, discard it.
    /// 2. The output batch must not be visible until `batch_end` is called.
    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
        Ok(())  // Default implementation does nothing
    }

    /// Push a buffer of encoded data to the output.
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()>;

    /// Output a message consisting of a key/value pair, with optional headers.
    /// 
    /// This API is implemented by transports that transmit messages with
    /// key and value fields (e.g., Kafka, Redis).
    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()>;

    /// Notifies the output endpoint that output for the current step is complete.
    /// 
    /// A fault-tolerant output endpoint may now make the output batch visible
    /// to readers.
    fn batch_end(&mut self) -> AnyResult<()> {
        Ok(())  // Default implementation does nothing
    }

    /// Whether this endpoint is fault tolerant.
    fn is_fault_tolerant(&self) -> bool;

    /// Returns the approximate amount of memory used by the connector's
    /// underlying implementation.
    fn memory(&self) -> usize {
        0  // Default: no memory tracking
    }
}
```

### Naming and `TransportConfig` JSON Tags

Transport connectors are selected via the `TransportConfig` enum which uses:

```rust
#[serde(tag = "name", content = "config", rename_all = "snake_case")]
```

This means the JSON/YAML `transport.name` string is derived from the **enum variant name**, converted to snake_case.

Example:
- Variant `MongodbOutput(...)` -> `"name": "mongodb_output"`
- Variant `MongoDbOutput(...)` -> `"name": "mongo_db_output"`

When documenting a connector name, make sure the string matches the chosen variant name.

### Key Types

```rust
/// A step number for fault-tolerant output.
/// The first step is numbered zero.
pub type Step = u64;

/// Callback for reporting asynchronous errors.
/// Parameters: (is_fatal, error, optional_error_tag)
pub type AsyncErrorCallback = Box<dyn Fn(bool, AnyError, Option<&'static str>) + Send + Sync>;
```

---

## Implementing a Non-Fault-Tolerant Connector

Non-fault-tolerant connectors are the simplest to implement. Here's a complete example based on the File output connector.

> **Tip**: Use the [output connector template](./template/output_connector_template.rs) as a starting point. It includes both non-FT and FT implementations with comprehensive comments. See the [template README](./template/README.md) for usage instructions.

### Step 1: Define Your Configuration Type

In `/crates/feldera-types/src/transport/`, create or update a configuration struct:

```rust
// In /crates/feldera-types/src/transport/file.rs
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct FileOutputConfig {
    /// Path to the output file.
    pub path: String,
}
```

### Step 2: Implement the OutputEndpoint Trait

```rust
// In /crates/adapters/src/transport/file.rs
use anyhow::{bail, AnyError, Result as AnyResult};
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint};
use feldera_types::transport::file::FileOutputConfig;
use std::fs::File;
use std::io::Write;

pub(crate) struct FileOutputEndpoint {
    file: File,
}

impl FileOutputEndpoint {
    pub(crate) fn new(config: FileOutputConfig) -> AnyResult<Self> {
        let file = File::create(&config.path).map_err(|e| {
            AnyError::msg(format!(
                "Failed to create output file '{}': {e}",
                config.path
            ))
        })?;
        Ok(Self { file })
    }
}

impl OutputEndpoint for FileOutputEndpoint {
    fn connect(
        &mut self,
        _async_error_callback: AsyncErrorCallback,
    ) -> AnyResult<()> {
        // File is already opened in new(), nothing more to do
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        // No limit for file output
        usize::MAX
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        self.file.write_all(buffer)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        // File output doesn't support key-value pairs
        bail!(
            "File output transport does not support key-value pairs. \
            Use a format that produces plain buffers instead."
        );
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
```

### Key Points for Non-FT Connectors

1. **`is_fault_tolerant()` returns `false`** - This tells Feldera not to expect replay handling.

2. **Use default implementations** - `batch_start()` and `batch_end()` can use the default no-op implementations.

3. **`push_buffer()` is the main method** - Most data will flow through this method.

4. **`push_key()` should return an error** - Unless your connector specifically supports key-value pairs.

---

## Implementing a Fault-Tolerant Connector

Fault-tolerant connectors must track step numbers and handle replays. Here's an annotated example based on the Kafka FT connector.

> **Tip**: The [output connector template](./template/output_connector_template.rs) includes a complete fault-tolerant implementation with state machine, replay detection, and transaction handling. See Section 4 of the template.

### Step 1: Define State Management

```rust
/// State of the output endpoint.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum State {
    /// Just created, `connect()` not yet called.
    New,

    /// `connect()` has been called.
    Connected,

    /// `batch_start()` has been called for a step.
    /// We're currently writing data for this step.
    BatchOpen(Step),

    /// `batch_end()` has been called for the given step.
    BatchClosed(Step),
}

pub struct MyFaultTolerantEndpoint {
    // Connection to external system
    client: MyClient,
    
    // The next step number we expect to write.
    // Any step < next_step has already been written and should be discarded.
    next_step: Step,
    
    // Current state of the endpoint
    state: State,
    
    // Buffer for transaction
    pending_data: Vec<u8>,
}
```

### Step 2: Implement the Trait with Replay Detection

```rust
impl OutputEndpoint for MyFaultTolerantEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        debug_assert_eq!(self.state, State::New);
        
        // Connect to external system
        self.client.connect()?;
        
        // Read the last committed step from the external system
        // This is crucial for replay detection!
        self.next_step = self.client.read_last_committed_step()? + 1;
        
        self.state = State::Connected;
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        1_000_000 // 1MB, adjust based on your system's limits
    }

    fn batch_start(&mut self, step: Step) -> AnyResult<()> {
        match self.state {
            State::New => unreachable!("connect() should be called first"),
            State::Connected => {}
            State::BatchClosed(closed_step) => {
                if step <= closed_step {
                    unreachable!("step numbers should always increase");
                }
            }
            State::BatchOpen(_) => {
                unreachable!("batch_end() should be called before next batch_start()");
            }
        }

        // Check if this is a replay of already-written data
        if step >= self.next_step {
            // This is new data - begin a transaction
            self.client.begin_transaction()?;
        } else {
            // This is a replay - we'll discard this data
            info!(
                "Discarding step {} (already written, next_step={})",
                step, self.next_step
            );
        }

        self.state = State::BatchOpen(step);
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let State::BatchOpen(step) = self.state else {
            unreachable!("batch_start() should be called before push_buffer()");
        };

        // Only write if this is not a replay
        if step >= self.next_step {
            self.client.write(buffer)?;
        }
        // If step < next_step, silently discard the data
        
        Ok(())
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let State::BatchOpen(step) = self.state else {
            unreachable!("batch_start() should be called before push_key()");
        };

        if step >= self.next_step {
            self.client.write_key_value(key, val, headers)?;
        }
        
        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        let State::BatchOpen(step) = self.state else {
            unreachable!("batch_start() should be called before batch_end()");
        };

        if step >= self.next_step {
            // Commit the transaction - this makes data visible to readers
            self.client.commit_transaction()?;
            self.next_step = step + 1;
        }

        self.state = State::BatchClosed(step);
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        true
    }

    fn memory(&self) -> usize {
        self.client.memory_usage()
    }
}
```

### Key Points for Fault-Tolerant Connectors

1. **`is_fault_tolerant()` returns `true`** - This tells Feldera to expect replay handling.

2. **Track `next_step`** - On connect, read the last committed step from your external system.

3. **Discard replays** - If `step < next_step`, the data has already been written; discard it silently.

4. **Use transactions** - Data should not be visible until `batch_end()` commits it.

5. **State machine** - Use a state machine to catch programming errors (calling methods in wrong order).

### Recovery Flow

```
Pipeline starts
       │
       ▼
  connect() reads last committed step from external system
       │
       ├──▶ last_step = 5, so next_step = 6
       │
       ▼
  batch_start(step=3)  ◀── Replay! step < next_step
       │
       └──▶ Discard mode - no transaction started
       │
       ▼
  push_buffer() ──▶ Data discarded
       │
       ▼
  batch_end() ──▶ No commit (nothing written)
       │
       ▼
  batch_start(step=6)  ◀── New data! step >= next_step
       │
       └──▶ Begin transaction
       │
       ▼
  push_buffer() ──▶ Write data
       │
       ▼
  batch_end() ──▶ Commit transaction, next_step = 7
```

---

## Key-Value Connectors

Some systems (Redis, Kafka with Debezium) work with key-value pairs instead of plain buffers. Here's an example based on the Redis connector:

```rust
pub struct RedisOutputEndpoint {
    config: ConnectionInfo,
    pool: Option<r2d2::Pool<redis::Client>>,
    pipeline: Option<redis::Pipeline>,
}

impl OutputEndpoint for RedisOutputEndpoint {
    fn connect(&mut self, _: AsyncErrorCallback) -> AnyResult<()> {
        let client = redis::Client::open(self.config.clone())?;
        let pool = r2d2::Pool::builder().build(client)?;
        self.pool = Some(pool);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
        // Create an atomic pipeline (transaction)
        let mut pipeline = Pipeline::new();
        pipeline.atomic();
        self.pipeline = Some(pipeline);
        Ok(())
    }

    fn push_buffer(&mut self, _: &[u8]) -> AnyResult<()> {
        // Redis uses key-value pairs, not buffers
        bail!("Redis connector requires a key-value format (e.g., 'format: raw')")
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let key = key.ok_or(anyhow!("Redis requires a key"))?;
        let pipeline = self.pipeline.as_mut().ok_or(anyhow!(
            "push_key called before batch_start"
        ))?;

        if let Some(val) = val {
            // SET key value
            pipeline.set(key, val);
        } else {
            // DEL key (value is None means delete)
            pipeline.del(key);
        }

        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        let mut conn = self.pool.as_ref().unwrap().get()?;
        let pipeline = std::mem::take(&mut self.pipeline).unwrap();
        pipeline.exec(&mut conn)?;
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false  // Redis connector doesn't track steps
    }
}
```

### Key Points for Key-Value Connectors

1. **`push_buffer()` should return an error** - Direct users to use the correct format.

2. **Handle `val = None`** - This typically means "delete this key" (for change data capture).

3. **Headers are optional** - Many systems ignore them; only Kafka really uses them.

---

## Integrated Output Endpoints

Integrated output endpoints combine transport and encoding in a single implementation. This is useful for systems like databases where the encoding is tightly coupled to the transport (e.g., PostgreSQL with prepared statements, Delta Lake with Arrow/Parquet).

### When to Use Integrated Endpoints

Use integrated endpoints when:
- The target system has a native format (SQL, Parquet, Arrow)
- Encoding and transport are tightly coupled
- You need access to record-level metadata (insert/delete/upsert operations)
- Standard formats like JSON/CSV would be inefficient

### The IntegratedOutputEndpoint Trait

```rust
/// An output endpoint that implements its own encoder.
pub trait IntegratedOutputEndpoint: OutputEndpoint + Encoder {
    fn into_encoder(self: Box<Self>) -> Box<dyn Encoder>;
    fn as_endpoint(&mut self) -> &mut dyn OutputEndpoint;
}
```

The trait has a blanket implementation for any type implementing both `OutputEndpoint` and `Encoder`:

```rust
impl<T: OutputEndpoint + Encoder + 'static> IntegratedOutputEndpoint for T {
    fn into_encoder(self: Box<Self>) -> Box<dyn Encoder> { self }
    fn as_endpoint(&mut self) -> &mut dyn OutputEndpoint { self }
}
```

### The Encoder Trait

Integrated endpoints must also implement the `Encoder` trait:

```rust
pub trait Encoder: Send {
    /// Returns the consumer that receives encoded data.
    fn consumer(&mut self) -> &mut dyn OutputConsumer;

    /// Encode a batch of updates using the cursor.
    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()>;
}
```

### The OutputConsumer Trait

The `OutputConsumer` is similar to `OutputEndpoint` but includes record counts:

```rust
pub trait OutputConsumer: Send {
    fn max_buffer_size_bytes(&self) -> usize;
    fn batch_start(&mut self, step: Step);
    fn push_buffer(&mut self, buffer: &[u8], num_records: usize);
    fn push_key(&mut self, key: Option<&[u8]>, val: Option<&[u8]>, 
                headers: &[(&str, Option<&[u8]>)], num_records: usize);
    fn batch_end(&mut self);
}
```

### Example: PostgreSQL Integrated Connector

The PostgreSQL connector demonstrates key integrated endpoint patterns:

```rust
pub struct PostgresOutputEndpoint {
    client: Option<postgres::Client>,
    transaction: Option<postgres::Transaction<'static>>,
    prepared_statements: Option<PreparedStatements>,
    
    // Buffers for batching operations
    insert_buffer: Vec<u8>,
    upsert_buffer: Vec<u8>,
    delete_buffer: Vec<u8>,
    
    config: PostgresWriterConfig,
}

struct PreparedStatements {
    insert: Statement,
    upsert: Statement,
    delete: Statement,
}
```

**Key patterns:**

1. **Prepared Statements**: Pre-compile SQL for efficiency
2. **Buffer Batching**: Accumulate records as JSON arrays, flush when full
3. **Indexed Operations**: Track insert/update/delete separately

```rust
impl Encoder for PostgresOutputEndpoint {
    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut cursor = batch.cursor(RecordFormat::Json(JsonFlavor::Default))?;
        
        while cursor.key_valid() {
            // Determine operation type based on cursor weights
            match indexed_operation_type(&mut cursor) {
                Some(IndexedOperationType::Insert) => {
                    self.buffer_insert(cursor.key())?;
                }
                Some(IndexedOperationType::Delete) => {
                    self.buffer_delete(cursor.key())?;
                }
                Some(IndexedOperationType::Upsert) => {
                    self.buffer_upsert(cursor.key())?;
                }
                None => {} // Skip zero-weight records
            }
            cursor.step_key();
        }
        self.flush_all_buffers()?;
        Ok(())
    }
}
```

### Configuration: No Format Section

**Important**: Integrated endpoints handle their own encoding, so the connector configuration must NOT include a `format` section. This is enforced during pipeline construction.

---

## Registering Your Connector

To make your connector available in Feldera, you need to register it in several places.

> **Tip**: The [template README](./template/README.md) provides a detailed step-by-step checklist for registration, including exact file paths and code snippets to add.

### Step 1: Add Configuration to TransportConfig

In `/crates/feldera-types/src/config.rs`, add your config to the `TransportConfig` enum:

```rust
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "name", content = "config", rename_all = "snake_case")]
pub enum TransportConfig {
    FileInput(FileInputConfig),
    FileOutput(FileOutputConfig),
    KafkaOutput(KafkaOutputConfig),
    RedisOutput(RedisOutputConfig),
    // Add your new connector here:
    MySystemOutput(MySystemOutputConfig),
    // ... other variants
}

impl TransportConfig {
    pub fn name(&self) -> String {
        match self {
            TransportConfig::FileOutput(_) => "file_output".to_string(),
            TransportConfig::MySystemOutput(_) => "my_system_output".to_string(),
            // ... other matches
        }
    }
}
```

### Step 2: Register in Endpoint Factory

In `/crates/adapters/src/transport.rs`, add your connector to `output_transport_config_to_endpoint()`:

```rust
pub fn output_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
    secrets_dir: &Path,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    match config {
        TransportConfig::FileOutput(config) => {
            Ok(Some(Box::new(FileOutputEndpoint::new(config)?)))
        }
        
        // Add your connector here:
        TransportConfig::MySystemOutput(config) => {
            Ok(Some(Box::new(MySystemOutputEndpoint::new(config)?)))
        }
        
        // For connectors with both FT and non-FT variants:
        TransportConfig::KafkaOutput(config) => match fault_tolerant {
            false => Ok(Some(Box::new(KafkaNonFtOutputEndpoint::new(config)?))),
            true => Ok(Some(Box::new(KafkaFtOutputEndpoint::new(config)?))),
        },
        
        _ => Ok(None),
    }
}
```

### Integrated Output Connectors: Registration Path Is Different

If you're implementing an **integrated output connector** (one that implements both `OutputEndpoint` and `Encoder`), you generally do **not** register it via
`/crates/adapters/src/transport.rs`.

Instead, add a new match arm in `/crates/adapters/src/integrated.rs` inside `create_integrated_output_endpoint(...)`, alongside the existing integrated connectors
(Postgres, Delta Lake). The transport variant still lives in `TransportConfig`, but endpoint construction is routed through the integrated factory.

Integrated connectors must not allow a `format` section in the connector config. This is enforced in `create_integrated_output_endpoint(...)`.

### Step 3: Add Feature Flag (Optional)

If your connector has external dependencies, add a feature flag in `/crates/adapters/Cargo.toml`:

```toml
[features]
default = ["with-kafka", "with-redis", "with-my-system"]
with-my-system = ["my-system-client"]

[dependencies]
my-system-client = { version = "1.0", optional = true }
```

Then gate your code:

```rust
#[cfg(feature = "with-my-system")]
TransportConfig::MySystemOutput(config) => {
    Ok(Some(Box::new(MySystemOutputEndpoint::new(config)?)))
}
```

---

## Tracing and Logging

Consistent tracing helps with debugging and monitoring. Feldera connectors use the `tracing` crate for structured logging.

### Standard Span Pattern

Create a helper method that returns an entered span:

```rust
use tracing::{info_span, span::EnteredSpan};

impl MyOutputEndpoint {
    fn span(&self) -> EnteredSpan {
        info_span!(
            "my_system_output",
            ft = self.is_fault_tolerant(),
            endpoint_id = self.endpoint_id,
            endpoint_name = self.endpoint_name,
            // Add connector-specific fields
            target = self.config.target,
        ).entered()
    }
}
```

### Using Spans in Methods

Wrap method implementations with span guards:

```rust
fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
    let _guard = self.span();
    
    // Implementation here - all logs will be associated with the span
    debug!("Pushing {} bytes", buffer.len());
    self.client.send(buffer)?;
    
    Ok(())
}

fn batch_start(&mut self, step: Step) -> AnyResult<()> {
    let _guard = self.span();
    
    if step >= self.next_step {
        info!("Starting new batch for step {step}");
        self.client.begin_transaction()?;
    } else {
        info!("Discarding replay of step {step} (next_step={})", self.next_step);
    }
    
    Ok(())
}
```

### Span Fields by Connector Type

| Connector | Recommended Span Fields |
|-----------|------------------------|
| File | `path` |
| Kafka | `ft`, `topic` |
| Redis | `ft`, `connection_string` (sanitized) |
| PostgreSQL | `ft`, `id`, `name`, `table` |
| HTTP | `endpoint_id` |

### Deferred Logging Pattern (Kafka)

The Kafka connector uses deferred logging to capture librdkafka messages during initialization:

```rust
pub(crate) struct DeferredLogging(Mutex<Option<Vec<(RDKafkaLogLevel, String, String)>>>);

impl DeferredLogging {
    pub fn with_deferred_logging<F, R>(&self, f: F) -> R {
        *self.0.lock().unwrap() = Some(Vec::new());
        let result = f();
        
        // Now emit all captured logs
        for (level, fac, message) in self.0.lock().unwrap().take().unwrap().drain(..) {
            tracing::info!("{level:?} {fac} {message}");
        }
        result
    }
    
    pub fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if let Some(ref mut logs) = *self.0.lock().unwrap() {
            logs.push((level, fac.to_string(), log_message.to_string()));
        } else {
            // Direct logging when not deferring
            tracing::info!("{level:?} {fac} {log_message}");
        }
    }
}
```

---

## Memory Tracking

The `memory()` method allows connectors to report their memory usage. This is important for connectors that use significant memory in their underlying libraries.

### When to Implement Memory Tracking

Implement `memory()` when your connector:
- Uses a library with internal buffering (e.g., librdkafka)
- Maintains large in-memory buffers
- Allocates significant memory outside of Rust's allocator

### Example: Kafka Memory Reporter

```rust
struct MemoryUseReporter {
    start: Instant,
    current: u64,
    peak: Option<(Instant, u64)>,
}

impl MemoryUseReporter {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            current: 0,
            peak: None,
        }
    }
    
    fn update(&mut self, statistics: &rdkafka::Statistics) {
        let mut memory = 0;
        
        // Sum memory across all topic partitions
        for topic in statistics.topics.values() {
            for partition in topic.partitions.values() {
                memory += partition.msgq_bytes      // Message queue
                       + partition.xmit_msgq_bytes  // Transmit queue
                       + partition.fetchq_size;     // Fetch queue
            }
        }
        
        self.current = memory;
        
        // Log if memory increased by 50%
        if let Some((_, last_peak)) = self.peak {
            if memory > last_peak * 3 / 2 {
                info!("Memory usage increased to {} bytes", memory);
                self.peak = Some((Instant::now(), memory));
            }
        } else if memory > 0 {
            self.peak = Some((Instant::now(), memory));
        }
    }
    
    fn current(&self) -> usize {
        self.current as usize
    }
}
```

### Using with ClientContext (Kafka)

```rust
impl ClientContext for MyContext {
    fn stats(&self, statistics: rdkafka::Statistics) {
        self.memory_use_reporter.lock().unwrap().update(&statistics);
    }
}

impl OutputEndpoint for MyKafkaEndpoint {
    fn memory(&self) -> usize {
        self.producer.context().memory_use_reporter.lock().unwrap().current()
    }
}
```

---

## Advanced Patterns

### Async Worker Pattern

For connectors that need async operations, use a worker thread with channels:

```rust
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;

enum Command {
    BatchStart(Step),
    Insert(Vec<u8>),
    BatchEnd,
    Shutdown,
}

struct AsyncOutputEndpoint {
    command_sender: Sender<Command>,
    response_receiver: Receiver<Result<(), AnyError>>,
    worker_handle: Option<thread::JoinHandle<()>>,
}

impl AsyncOutputEndpoint {
    fn new(config: Config) -> AnyResult<Self> {
        let (command_tx, command_rx) = channel::<Command>();
        let (response_tx, response_rx) = channel::<Result<(), AnyError>>();
        
        let handle = thread::Builder::new()
            .name("my-output-worker".to_string())
            .spawn(move || {
                // Create async runtime in worker thread
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    Self::worker_loop(config, command_rx, response_tx).await
                });
            })?;
        
        Ok(Self {
            command_sender: command_tx,
            response_receiver: response_rx,
            worker_handle: Some(handle),
        })
    }
    
    async fn worker_loop(
        config: Config,
        commands: Receiver<Command>,
        responses: Sender<Result<(), AnyError>>,
    ) {
        let mut client = AsyncClient::connect(&config).await.unwrap();
        
        while let Ok(cmd) = commands.recv() {
            let result = match cmd {
                Command::BatchStart(step) => client.begin_transaction().await,
                Command::Insert(data) => client.insert(&data).await,
                Command::BatchEnd => client.commit().await,
                Command::Shutdown => break,
            };
            let _ = responses.send(result.map_err(|e| anyhow!(e)));
        }
    }
}

impl OutputEndpoint for AsyncOutputEndpoint {
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        self.command_sender.send(Command::Insert(buffer.to_vec()))?;
        self.response_receiver.recv()?
    }
}
```

### Retry with Exponential Backoff

For transient errors, implement retry logic:

```rust
fn retry_with_backoff<F, T>(&mut self, operation: F) -> AnyResult<T>
where
    F: Fn(&mut Self) -> Result<T, BackoffError>,
{
    let mut backoff_ms = 1000;
    let max_backoff_ms = 60_000;
    let mut attempts = 0;
    
    loop {
        match operation(self) {
            Ok(result) => return Ok(result),
            Err(BackoffError::Permanent(e)) => return Err(e),
            Err(BackoffError::Temporary(e)) => {
                attempts += 1;
                warn!("Temporary error (attempt {}): {}", attempts, e);
                
                std::thread::sleep(Duration::from_millis(backoff_ms));
                backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                
                // Optionally reconnect
                self.reconnect()?;
            }
        }
    }
}

enum BackoffError {
    Temporary(AnyError),
    Permanent(AnyError),
}
```

### Classifying Errors for Retry

```rust
impl From<postgres::Error> for BackoffError {
    fn from(error: postgres::Error) -> Self {
        // These SQL states indicate transient errors
        const TEMPORARY_STATES: &[SqlState] = &[
            SqlState::CONNECTION_FAILURE,
            SqlState::CONNECTION_EXCEPTION,
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CRASH_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::SYSTEM_ERROR,
            SqlState::IO_ERROR,
        ];
        
        if error.is_closed() 
            || error.code().is_some_and(|c| TEMPORARY_STATES.contains(c))
            || error.code().is_none() // OS-level connection refused
        {
            BackoffError::Temporary(error.into())
        } else {
            BackoffError::Permanent(error.into())
        }
    }
}
```

### Connection Pooling

For connectors that need multiple connections, use a connection pool:

```rust
use r2d2::{Pool, PooledConnection};

struct PooledOutputEndpoint {
    pool: Pool<MyConnectionManager>,
}

impl PooledOutputEndpoint {
    fn new(config: Config) -> AnyResult<Self> {
        let manager = MyConnectionManager::new(&config);
        let pool = Pool::builder()
            .max_size(10)
            .connection_timeout(Duration::from_secs(30))
            .build(manager)?;
        
        Ok(Self { pool })
    }
    
    fn get_connection(&self) -> AnyResult<PooledConnection<MyConnectionManager>> {
        self.pool.get().map_err(|e| anyhow!("Failed to get connection: {e}"))
    }
}

impl OutputEndpoint for PooledOutputEndpoint {
    fn batch_end(&mut self) -> AnyResult<()> {
        let mut conn = self.get_connection()?;
        // Use connection...
        Ok(())
    }
}
```

### Chunking Large Batches

For systems with record limits, chunk your data:

```rust
const CHUNK_SIZE: usize = 100_000;

fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
    let mut cursor = batch.cursor(RecordFormat::Json(JsonFlavor::Default))?;
    let mut record_count = 0;
    
    while cursor.key_valid() {
        self.buffer_record(cursor.key())?;
        record_count += 1;
        
        // Flush when chunk is full
        if record_count >= CHUNK_SIZE {
            self.flush_buffer()?;
            record_count = 0;
        }
        
        cursor.step_key();
    }
    
    // Flush remaining records
    if record_count > 0 {
        self.flush_buffer()?;
    }
    
    Ok(())
}
```

---

## Error Handling

### Synchronous Errors

Return errors from trait methods using `anyhow::Result`:

```rust
fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
    self.client.send(buffer).map_err(|e| {
        anyhow!("Failed to send data to MySystem: {e}")
    })?;
    Ok(())
}
```

### Asynchronous Errors

Use the `async_error_callback` for errors that occur outside method calls (e.g., delivery failures in Kafka):

```rust
fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
    // Store the callback for later use
    self.error_callback = Some(async_error_callback);
    
    // Set up an error handler in your client
    self.client.on_error(|error| {
        if let Some(cb) = &self.error_callback {
            let is_fatal = error.is_unrecoverable();
            cb(is_fatal, anyhow!("{error}"), Some("my_system_error"));
        }
    });
    
    Ok(())
}
```

### Error Callback Parameters

```rust
async_error_callback(
    is_fatal: bool,           // true = endpoint cannot recover
    error: AnyError,          // the error
    tag: Option<&'static str> // optional error category for metrics
);
```

---

## Secrets and Security

### Secret Resolution

Feldera supports secret references in connector configurations. Secrets are resolved before the configuration is passed to your connector:

```rust
pub fn output_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
    secrets_dir: &Path,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    // Secrets are resolved here before your connector sees the config
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    // ...
}
```

### SSL/TLS Certificate Handling

For connectors that need SSL/TLS, support both PEM strings and file paths:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SslConfig {
    /// CA certificate as PEM string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_ca_pem: Option<String>,
    
    /// Path to CA certificate file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_ca_location: Option<String>,
    
    /// Client certificate as PEM string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_client_pem: Option<String>,
    
    /// Path to client certificate file  
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_client_location: Option<String>,
    
    /// Whether to verify server hostname
    #[serde(default = "default_true")]
    pub verify_hostname: bool,
}

fn configure_ssl(builder: &mut SslConnectorBuilder, config: &SslConfig) -> AnyResult<()> {
    // Load CA certificate
    if let Some(ref pem) = config.ssl_ca_pem {
        let cert = X509::from_pem(pem.as_bytes())?;
        builder.cert_store_mut().add_cert(cert)?;
    } else if let Some(ref path) = config.ssl_ca_location {
        builder.set_ca_file(path)?;
    }
    
    // Load client certificate
    if let Some(ref pem) = config.ssl_client_pem {
        let cert = X509::from_pem(pem.as_bytes())?;
        builder.set_certificate(&cert)?;
    }
    
    // Configure hostname verification
    if !config.verify_hostname {
        warn!("SSL hostname verification is disabled - this is insecure!");
        builder.set_verify(SslVerifyMode::NONE);
    }
    
    Ok(())
}
```

### PEM-to-File Workaround (Kafka/librdkafka)

Some libraries (like librdkafka) only accept file paths for certificates. Use a workaround:

```rust
pub(crate) trait PemToLocation {
    fn pem_to_location(&mut self, endpoint_name: &str) -> AnyResult<()>;
}

impl PemToLocation for rdkafka::ClientConfig {
    fn pem_to_location(&mut self, endpoint_name: &str) -> AnyResult<()> {
        // If ssl.certificate.pem is set, save it to a temp file
        if let Some(pem) = self.get("ssl.certificate.pem") {
            let temp_dir = std::env::temp_dir();
            let file_path = temp_dir.join(format!("{endpoint_name}-cert.pem"));
            std::fs::write(&file_path, pem)?;
            
            // Remove the PEM string and set the location instead
            self.set("ssl.certificate.location", file_path.to_string_lossy());
            // Note: The PEM value should be removed from config
        }
        Ok(())
    }
}
```

### Security Best Practices

1. **Never log secrets**: Be careful not to log connection strings or credentials
   ```rust
   fn span(&self) -> EnteredSpan {
       info_span!(
           "my_output",
           // DON'T: connection_string = self.config.connection_string,
           // DO: sanitize or omit sensitive fields
           host = self.config.host,
       ).entered()
   }
   ```

2. **Validate SSL configuration**: Warn users about insecure configurations
   ```rust
   if !config.verify_hostname {
       warn!("Disabling hostname verification is insecure and should only be used for testing");
   }
   ```

3. **Handle OAuth tokens securely**: For systems like AWS MSK
   ```rust
   impl ClientContext for MyContext {
       const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;
       
       fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
           // Generate token using AWS credentials
           generate_oauthbearer_token(&self.oauthbearer_config)
       }
   }
   ```

4. **Clean up temporary files**: If you write certificates to disk
   ```rust
   impl Drop for MyEndpoint {
       fn drop(&mut self) {
           if let Some(ref path) = self.temp_cert_path {
               let _ = std::fs::remove_file(path);
           }
       }
   }
   ```

---

## Testing Guidelines

### Unit Tests

Test your connector in isolation:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_output() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("output.txt");
        
        let config = FileOutputConfig {
            path: path.to_string_lossy().to_string(),
        };
        
        let mut endpoint = FileOutputEndpoint::new(config).unwrap();
        endpoint.connect(Box::new(|_, _, _| {})).unwrap();
        
        endpoint.batch_start(0).unwrap();
        endpoint.push_buffer(b"hello").unwrap();
        endpoint.batch_end().unwrap();
        
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "hello");
    }
}
```

### Integration Tests

For fault-tolerant connectors, test replay behavior:

```rust
#[test]
fn test_replay_detection() {
    let mut endpoint = create_test_endpoint();
    endpoint.connect(Box::new(|_, _, _| {})).unwrap();
    
    // Write step 0
    endpoint.batch_start(0).unwrap();
    endpoint.push_buffer(b"data0").unwrap();
    endpoint.batch_end().unwrap();
    
    // Simulate restart - create new endpoint that reads last committed step
    let mut endpoint2 = create_test_endpoint();
    endpoint2.connect(Box::new(|_, _, _| {})).unwrap();
    
    // Replay step 0 - should be discarded
    endpoint2.batch_start(0).unwrap();
    endpoint2.push_buffer(b"data0-replay").unwrap();
    endpoint2.batch_end().unwrap();
    
    // Write step 1 - should succeed
    endpoint2.batch_start(1).unwrap();
    endpoint2.push_buffer(b"data1").unwrap();
    endpoint2.batch_end().unwrap();
    
    // Verify only data0 and data1 exist, not data0-replay
    assert_eq!(read_all_data(), vec!["data0", "data1"]);
}
```

### Docker-Based Tests

For connectors that need external services, use Docker:

```rust
#[test]
#[ignore] // Run with: cargo test -- --ignored
fn test_kafka_integration() {
    // Assumes Kafka is running on localhost:9092
    let config = KafkaOutputConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        topic: "test-topic".to_string(),
        // ...
    };
    
    // Run your tests...
}
```

---

## Summary

| Connector Type | `is_fault_tolerant()` | `batch_start()` | `batch_end()` | Key Requirement |
|---------------|----------------------|-----------------|---------------|-----------------|
| Non-FT | `false` | Default OK | Default OK | Just write data |
| Fault-Tolerant | `true` | Track step, detect replay | Commit transaction | Discard replays, use transactions |
| Key-Value | Either | Either | Either | Implement `push_key()`, error on `push_buffer()` |

### Quick Checklist

- [ ] Configuration type in `feldera-types`
- [ ] Implement `OutputEndpoint` trait
- [ ] Add to `TransportConfig` enum
- [ ] Register in `output_transport_config_to_endpoint()`
- [ ] Add feature flag if needed
- [ ] Write unit tests
- [ ] Write integration tests
- [ ] Update documentation

---

## Connector Feature Matrix

This matrix shows the capabilities of each built-in output connector:

| Feature | File | HTTP | Redis | Kafka (non-FT) | Kafka (FT) | PostgreSQL | Delta Lake |
|---------|------|------|-------|----------------|------------|------------|------------|
| **Type** | Transport | Transport | Transport | Transport | Transport | Integrated | Integrated |
| **Fault Tolerant** | No | No | No | No | **Yes** | No | No |
| **push_buffer()** | Yes | Yes | No | Yes | Yes | N/A | N/A |
| **push_key()** | No | No | **Yes** | Yes | No | N/A | N/A |
| **Connection Pool** | N/A | N/A | Yes (r2d2) | No | No | No | N/A |
| **Transactions** | No | No | Yes | No | **Yes** | Yes | Yes |
| **Memory Tracking** | No | No | No | **Yes** | **Yes** | No | No |
| **Backpressure** | No | **Yes** | No | No | No | No | No |
| **Tracing Span** | No | Yes | Yes | Yes | Yes | Yes | No |
| **Retry Logic** | No | No | No | Yes | Yes | **Yes** | Yes |
| **Async Worker** | No | Yes | No | No | No | No | **Yes** |
| **SSL/TLS** | N/A | N/A | Yes | Yes | Yes | **Yes** | Via S3 |
| **OAuth** | N/A | N/A | No | Yes | Yes | No | Via AWS |

### Legend

- **Transport**: Uses separate encoder (JSON, CSV, etc.)
- **Integrated**: Combines transport and encoding
- **N/A**: Not applicable for this connector type
- **Bold**: Notable/advanced feature

### Choosing the Right Connector Type

```
                           ┌─────────────────────────────────┐
                           │ Does target system have a       │
                           │ native format (SQL, Parquet)?   │
                           └───────────────┬─────────────────┘
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    │ Yes                                    No   │
                    ▼                                             ▼
           ┌────────────────┐                           ┌────────────────┐
           │  Integrated    │                           │   Transport    │
           │  Endpoint      │                           │   Endpoint     │
           └────────────────┘                           └───────┬────────┘
                                                                │
                                              ┌─────────────────┴─────────────────┐
                                              │ Does system use key-value pairs?  │
                                              └─────────────────┬─────────────────┘
                                                                │
                                        ┌───────────────────────┴───────────────────────┐
                                        │ Yes                                       No  │
                                        ▼                                               ▼
                               ┌────────────────┐                              ┌────────────────┐
                               │  Implement     │                              │  Implement     │
                               │  push_key()    │                              │  push_buffer() │
                               └────────────────┘                              └────────────────┘
```

---

## References

### Templates and Quick Start
- **Output connector template**: [`docs/template/output_connector_template.rs`](./template/output_connector_template.rs)
- **Template usage guide**: [`docs/template/README.md`](./template/README.md)

### Core Traits and Types
- **OutputEndpoint trait**: `/crates/adapterlib/src/transport.rs`
- **IntegratedOutputEndpoint trait**: `/crates/adapters/src/integrated.rs`
- **Encoder trait**: `/crates/adapters/src/format/mod.rs`
- **Config types**: `/crates/feldera-types/src/config.rs`

### Transport Output Connectors
- **File output (simplest)**: `/crates/adapters/src/transport/file.rs`
- **HTTP output (streaming)**: `/crates/adapters/src/transport/http/output.rs`
- **Redis output (key-value)**: `/crates/adapters/src/transport/redis/output.rs`
- **Kafka non-FT output**: `/crates/adapters/src/transport/kafka/nonft/output.rs`
- **Kafka FT output (fault-tolerant)**: `/crates/adapters/src/transport/kafka/ft/output.rs`

### Integrated Output Connectors
- **PostgreSQL output**: `/crates/adapters/src/integrated/postgres/output.rs`
- **Delta Lake output**: `/crates/adapters/src/integrated/delta_table/output.rs`

### Registration and Configuration
- **Endpoint factory**: `/crates/adapters/src/transport.rs` (`output_transport_config_to_endpoint`)
- **Transport config enum**: `/crates/feldera-types/src/config.rs` (`TransportConfig`)
