# Feldera Connector Templates

This directory contains starter templates for implementing new Feldera connectors.

## Available Templates

| Template | Description |
|----------|-------------|
| [`output_connector_template.rs`](./output_connector_template.rs) | Output connector (sink/adapter) template with both FT and non-FT variants |

---

## Output Connector Template

### Quick Start

```bash
# 1. Create your connector directory
mkdir -p crates/adapters/src/transport/my_connector

# 2. Copy the template
cp docs/template/output_connector_template.rs crates/adapters/src/transport/my_connector/output.rs

# 3. Rename the placeholder (replace "mongodb" with your connector name)
sed -i '' 's/MyNewOutput/MongoDB/g' crates/adapters/src/transport/my_connector/output.rs
sed -i '' 's/my_new_output/mongodb/g' crates/adapters/src/transport/my_connector/output.rs

# GNU sed (Linux) equivalent:
# sed -i 's/MyNewOutput/MongoDB/g' crates/adapters/src/transport/my_connector/output.rs
# sed -i 's/my_new_output/mongodb/g' crates/adapters/src/transport/my_connector/output.rs

# 4. Open and follow the TODO comments
code crates/adapters/src/transport/my_connector/output.rs
```

### Step-by-Step Guide

#### Step 1: Copy and Rename the Template

Copy the template to your connector's directory:

```bash
# Example: Creating a MongoDB output connector
mkdir -p crates/adapters/src/transport/mongodb
cp docs/template/output_connector_template.rs crates/adapters/src/transport/mongodb/output.rs
```

Rename all placeholder names:

| Find | Replace With | Example |
|------|--------------|---------|
| `MyNewOutput` | Your connector name (PascalCase) | `MongoDB` |
| `my_new_output` | Your connector name (snake_case) | `mongodb` |
| `MY_NEW_OUTPUT` | Your connector name (SCREAMING_SNAKE_CASE) | `MONGODB` |

#### Step 2: Implement the Stub Client

The template includes a stub `MyNewOutputClient` struct. Replace it with your actual client implementation:

```rust
// Before (stub):
struct MyNewOutputClient {
    connected: bool,
}

// After (real implementation):
use mongodb::{Client, Collection};

struct MongoDBClient {
    client: Option<Client>,
    collection: Option<Collection<Document>>,
}
```

Key methods to implement:

| Method | Purpose |
|--------|---------|
| `new()` | Initialize client with config |
| `connect()` | Establish connection |
| `write()` | Write buffer data |
| `set()` / `delete()` | Key-value operations |
| `begin_transaction()` | Start transaction (FT only) |
| `commit_transaction()` | Commit transaction (FT only) |
| `read_last_committed_step()` | Read last step for replay detection (FT only) |

#### Step 3: Add Configuration to `feldera-types`

Create your config file:

```bash
# Create config file
touch crates/feldera-types/src/transport/mongodb.rs
```

Add your config struct:

```rust
// crates/feldera-types/src/transport/mongodb.rs
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct MongoDBOutputConfig {
    /// MongoDB connection string (e.g., "mongodb://localhost:27017")
    pub connection_string: String,
    
    /// Database name
    pub database: String,
    
    /// Collection name
    pub collection: String,
    
    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_secs: u32,
}

fn default_timeout() -> u32 {
    30
}
```

Export from the transport module:

```rust
// crates/feldera-types/src/transport/mod.rs
pub mod mongodb;
pub use mongodb::MongoDBOutputConfig;
```

#### Step 4: Add to `TransportConfig` Enum

Edit `/crates/feldera-types/src/config.rs`:

```rust
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "name", content = "config", rename_all = "snake_case")]
pub enum TransportConfig {
    // ... existing variants ...

    // Add your new connector:
    // NOTE: With `rename_all = "snake_case"`, `MongoDbOutput` becomes `mongo_db_output`.
    // Use that string in pipeline JSON/YAML.
    MongoDbOutput(MongoDBOutputConfig),
}

impl TransportConfig {
    pub fn name(&self) -> String {
        match self {
            // ... existing matches ...
            TransportConfig::MongoDbOutput(_) => "mongo_db_output".to_string(),
        }
    }
}
```

#### Step 5: Register in Endpoint Factory

Edit `/crates/adapters/src/transport.rs`:

```rust
use crate::transport::mongodb::{MongoDBOutputEndpoint, MongoDBFtOutputEndpoint};

pub fn output_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    fault_tolerant: bool,
    secrets_dir: &Path,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    let config = resolve_secret_references_via_json(secrets_dir, config)?;
    match config {
        // ... existing matches ...
        
        // For connectors with both FT and non-FT variants:
        TransportConfig::MongoDbOutput(config) => match fault_tolerant {
            false => Ok(Some(Box::new(MongoDBOutputEndpoint::new(config)?))),
            true => Ok(Some(Box::new(MongoDBFtOutputEndpoint::new(config)?))),
        },
        
        _ => Ok(None),
    }
}
```

#### Step 6: Create Module Structure

Create the module file:

```rust
// crates/adapters/src/transport/mongodb/mod.rs
mod output;

pub use output::{MongoDBOutputEndpoint, MongoDBFtOutputEndpoint};
```

Add to parent module:

```rust
// crates/adapters/src/transport/mod.rs
pub mod mongodb;
```

#### Step 7: Add Dependencies (Optional)

If your connector needs external crates, add them to `Cargo.toml`:

```toml
# crates/adapters/Cargo.toml

[features]
default = ["with-kafka", "with-redis", "with-mongodb"]  # Add your feature
with-mongodb = ["mongodb"]  # Gate behind feature flag

[dependencies]
mongodb = { version = "2.8", optional = true }
```

Gate your code with the feature flag:

```rust
// crates/adapters/src/transport.rs
#[cfg(feature = "with-mongodb")]
TransportConfig::MongoDbOutput(config) => { ... }
```

#### Step 8: Delete Unused Implementation

The template includes both fault-tolerant and non-fault-tolerant implementations. Delete the one you don't need:

- **Keep only non-FT**: Delete `MyNewOutputFtEndpoint` and `FtState`
- **Keep only FT**: Delete `MyNewOutputEndpoint` (the non-FT version)
- **Keep both**: If your system supports both modes

#### Step 9: Run Tests

```bash
# Run unit tests
cargo test -p adapters mongodb

# Run integration tests (requires external system)
MY_MONGODB_URL=mongodb://localhost:27017 cargo test -p adapters mongodb -- --ignored
```

### Template Structure

```
output_connector_template.rs
│
├── Section 1: Imports
│   └── Standard imports + TODO for external crates
│
├── Section 2: Stub Types (remove when implementing)
│   └── MyNewOutputClient with placeholder methods
│
├── Section 3: Configuration
│   └── MyNewOutputConfig with validation
│
├── Section 4: Non-Fault-Tolerant Endpoint
│   ├── MyNewOutputEndpoint struct
│   └── OutputEndpoint trait implementation
│
├── Section 5: Fault-Tolerant Endpoint
│   ├── FtState enum (state machine)
│   ├── MyNewOutputFtEndpoint struct
│   └── OutputEndpoint trait implementation with replay detection
│
├── Section 6: Registration Guide (comments only)
│   └── Step-by-step instructions
│
├── Section 7: Unit Tests
│   └── Test examples for config, non-FT, and FT
│
└── Section 8: Integration Tests
    └── Docker-based testing patterns
```

### Checklist

Use this checklist to track your implementation progress:

- [ ] Copy template to `crates/adapters/src/transport/<name>/output.rs`
- [ ] Rename all placeholders (`MyNewOutput` -> `YourName`)
- [ ] Replace stub client with real implementation
- [ ] Create config struct in `crates/feldera-types/src/transport/`
- [ ] Export config from `crates/feldera-types/src/transport/mod.rs`
- [ ] Add variant to `TransportConfig` enum in `crates/feldera-types/src/config.rs`
- [ ] Add `name()` match arm in `TransportConfig::name()`
- [ ] Register in `output_transport_config_to_endpoint()` in `crates/adapters/src/transport.rs`
- [ ] Create `mod.rs` in your connector directory
- [ ] Add module to `crates/adapters/src/transport/mod.rs`
- [ ] (Optional) Add feature flag in `Cargo.toml`
- [ ] (Optional) Delete unused FT or non-FT variant
- [ ] Implement all `// TODO:` comments
- [ ] Write unit tests
- [ ] Write integration tests
- [ ] Test with a real Feldera pipeline

### Common Patterns

#### Async Error Handling

```rust
fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
    self.error_callback = Some(async_error_callback);
    
    // Later, in a callback or background thread:
    if let Some(ref cb) = self.error_callback {
        cb(is_fatal, error, Some("my_connector_error"));
    }
}
```

#### Tracing Spans

```rust
fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
    let _guard = info_span!("mongodb_output", operation = "write").entered();
    debug!("Writing {} bytes", buffer.len());
    // ...
}
```

#### Replay Detection (FT)

```rust
fn batch_start(&mut self, step: Step) -> AnyResult<()> {
    if step < self.next_step {
        // Replay - discard this batch
        self.is_replay = true;
    } else {
        // New data - begin transaction
        self.client.begin_transaction()?;
        self.is_replay = false;
    }
    Ok(())
}
```

---

## Documentation References

- **Full Implementation Guide**: [docs/output-connector-guide.md](../output-connector-guide.md)
- **Core Trait Definition**: `/crates/adapterlib/src/transport.rs`
- **Example Implementations**:
  - Simple (File): `/crates/adapters/src/transport/file.rs`
  - Key-Value (Redis): `/crates/adapters/src/transport/redis/output.rs`
  - Fault-Tolerant (Kafka): `/crates/adapters/src/transport/kafka/ft/output.rs`

## Future Templates

Planned templates (not yet available):

- [ ] `input_connector_template.rs` - Input connector (source) template
- [ ] `integrated_connector_template.rs` - Integrated connector (combined transport + encoder)
