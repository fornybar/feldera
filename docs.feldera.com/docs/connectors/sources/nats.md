# NATS input connector

Feldera can consume a stream of changes to a SQL table from NATS JetStream
with the `nats_input` connector.

The NATS input connector supports exactly-once [fault
tolerance](/pipelines/fault-tolerance) using JetStream's ordered pull consumer.

:::warning

NATS support is still experimental, and it may be substantially modified in the future.

:::

## How it works

The NATS input connector uses JetStream's **ordered pull consumer**, which provides:
- **Strict ordering**: Messages delivered in exact stream order without gaps.
- **Automatic recovery**: On gap detection, heartbeat loss, or deletion, the consumer automatically recreates itself and resumes from the last processed position
- **Exactly-once semantics**: Combined with Feldera's checkpoint mechanism, ensures each message is processed exactly once

## NATS Input Connector Configuration

The connector configuration consists of three main sections:

### Connection Options

| Property                | Type   | Required | Description |
|------------------------|--------|----------|-------------|
| `server_url`           | string | Yes      | NATS server URL (e.g., `nats://localhost:4222`) |
| `auth`                 | object | No       | Authentication configuration (see [Authentication](#authentication)) |

### Stream Configuration

| Property      | Type   | Required | Description |
|--------------|--------|----------|-------------|
| `stream_name` | string | Yes      | The name of the NATS JetStream stream to consume from |

### Consumer Configuration

| Property           | Type                    | Required | Description |
|-------------------|-------------------------|----------|-------------|
| `name`            | string                  | No       | Consumer name for identification |
| `description`     | string                  | No       | Consumer description |
| `filter_subjects` | string list             | No       | Filter messages by subject(s). If empty, consumes all subjects in the stream |
| `replay_policy`   | variant                 | No       | Message replay speed: `"Instant"` (default, fast) or `"Original"` (rate-limited at original timing) |
| `rate_limit`      | integer                 | No       | Rate limit in bytes per second. Default: 0 (unlimited) |
| `deliver_policy`  | variant                 | Yes      | Starting point for reading from the stream (see [Deliver Policy](#deliver-policy)) |
| `max_waiting`     | integer                 | No       | Maximum outstanding pull requests. Default: 0 |
| `metadata`        | map (string → string)   | No       | Consumer metadata key-value pairs |
| `max_batch`       | integer                 | No       | Maximum messages per batch |
| `max_bytes`       | integer                 | No       | Maximum bytes per batch |
| `max_expires`     | duration                | No       | Maximum duration for pull requests |

### Metadata Options

| Property                  | Type    | Required | Description |
|---------------------------|---------|----------|-------------|
| `include_subject`         | boolean | No       | Whether to include the message subject in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_headers`         | boolean | No       | Whether to include message headers in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_stream`          | boolean | No       | Whether to include the stream name in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_consumer`        | boolean | No       | Whether to include the consumer name in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_stream_sequence` | boolean | No       | Whether to include the stream sequence number in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_consumer_sequence` | boolean | No     | Whether to include the consumer sequence number in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_delivered`       | boolean | No       | Whether to include the delivery attempt count in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_pending`         | boolean | No       | Whether to include the pending message count in connector metadata (see [Accessing NATS metadata](#metadata)) |
| `include_published`       | boolean | No       | Whether to include the publish timestamp in connector metadata (see [Accessing NATS metadata](#metadata)) |

#### Deliver Policy

The `deliver_policy` field determines where in the stream to start consuming messages:

- `"All"` - Start from the earliest available message in the stream
- `"Last"` - Start from the last message in the stream
- `"New"` - Start from new messages only (messages arriving after consumer creation)
- `"LastPerSubject"` - Start with the last message for all subjects received (useful for KV-like workloads)
- `{"ByStartSequence": {"start_sequence": 100}}` - Start from a specific sequence number
- `{"ByStartTime": {"start_time": "2024-01-01T12:00:00Z"}}` - Start from messages at or after the specified timestamp (RFC 3339 format)

#### Replay Policy

The `replay_policy` field controls how fast messages are delivered to the consumer:

- `"Instant"` (default) - Delivers messages as quickly as possible. Use for maximum throughput in production workloads.
- `"Original"` - Delivers messages at the rate they were originally received, preserving the timing between messages. Useful for:
  - Replaying production traffic patterns in test/staging environments
  - Load testing with realistic timing
  - Debugging scenarios where message timing matters

If not specified, defaults to `"Instant"`.

## Authentication

The NATS connector currently supports credentials-based authentication through the `auth` object.

### Credentials File Authentication

Use a credentials file containing JWT and NKey seed:

```json
{
  "credentials": {
    "FromFile": "/path/to/credentials.creds"
  }
}
```

Or provide credentials directly as a string:

```json
{
  "credentials": {
    "FromString": "-----BEGIN NATS USER JWT-----\n...\n------END NATS USER JWT------\n\n************************* IMPORTANT *************************\n..."
  }
}
```

:::note
Additional authentication methods (JWT, NKey, token, username/password) are defined in the configuration schema but not yet implemented. Only credentials-based authentication is currently supported.
:::

:::tip
For production environments, it is strongly recommended to use [secret references](/connectors/secret-references) instead of hardcoding credentials in the configuration.
:::

## Setting up NATS JetStream

Before using the NATS input connector, you need a NATS server with JetStream enabled and a stream created.

### Quickstart
The quickest way to start experimenting with Feldera and NATS is to use Docker Compose:

```bash
curl -L https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml -o docker-compose.yml
docker compose --profile nats up
```

This starts a Feldera pipeline manager, NATS server, and the NATS CLI. Connect to the CLI container with:

```bash
docker compose exec nats-cli sh
```

You can then easily publish messages to the NATS server using the `nats` CLI.

### Creating a Stream
Once installed, create a stream and publish test messages:

```bash
# Create a stream
nats stream add my_texts --subjects "text.>" --defaults

# Publish test messages
nats pub -J --count 100 text.area.1 '{"unix": {{UnixNano}}, "text": "{{Random 0 20}}"}'
```

## Example usage

### Basic example with raw JSON format

Create a NATS input connector that reads from the `my_texts` stream:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "All"
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
```

### Only receive new NATS messages

If you only want to receive messages published after the Feldera pipeline starts,
change `deliver_policy` to `New`.

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "New"
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
```

### Filtering by subject

Use `filter_subjects` to only consume messages from specific subjects `text.area.2` and `text.*.3`:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "All",
                     "filter_subjects": ["text.area.2", "text.*.3"]
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
```

### Replaying at original timing

You can use `"Original"` replay policy to replay production traffic in a test environment with realistic timing:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "All",
                    "replay_policy": "Original"
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
```
## <a name="metadata"></a>Accessing NATS metadata

NATS JetStream messages include several metadata attributes in addition to the payload. These can be extracted by the NATS connector and accessed from SQL:

| Metadata attribute     | SQL type                 | `CONNECTOR_METADATA()` field | Configuration option      |
|------------------------|--------------------------|------------------------------|---------------------------|
| Message subject        | `VARCHAR`                | `nats_subject`               | `include_subject`         |
| Message headers        | `MAP<STRING, VARBINARY>` | `nats_headers`               | `include_headers`         |
| Stream name            | `VARCHAR`                | `nats_stream`                | `include_stream`          |
| Consumer name          | `VARCHAR`                | `nats_consumer`              | `include_consumer`        |
| Stream sequence        | `BIGINT`                 | `nats_stream_sequence`       | `include_stream_sequence` |
| Consumer sequence      | `BIGINT`                 | `nats_consumer_sequence`     | `include_consumer_sequence` |
| Delivery attempts      | `BIGINT`                 | `nats_delivered`             | `include_delivered`       |
| Pending messages       | `BIGINT`                 | `nats_pending`               | `include_pending`         |
| Publish timestamp      | `TIMESTAMP`              | `nats_published`             | `include_published`       |

Some applications need to ingest and store these attributes alongside the message payload.
The steps below describe how to extract and use NATS metadata in SQL tables.

1. **Enable metadata extraction in the NATS connector.**
   Use the configuration options listed in the table above to enable only the metadata fields your application needs.
   Extracting unnecessary attributes adds overhead to ingestion and processing.

2. **Use metadata values to populate table columns.**
   Enabled metadata attributes are exposed via the `CONNECTOR_METADATA()` function, which returns a
   `VARIANT` containing a map with all selected attributes. You can reference these values in `DEFAULT`
   expressions to initialize table columns:

```sql
CREATE TABLE messages_with_metadata (
    unix BIGINT,
    text STRING,
    nats_subject VARCHAR DEFAULT CAST(CONNECTOR_METADATA()['nats_subject'] AS VARCHAR),
    nats_stream_sequence BIGINT DEFAULT CAST(CONNECTOR_METADATA()['nats_stream_sequence'] AS BIGINT),
    nats_published TIMESTAMP DEFAULT CAST(CONNECTOR_METADATA()['nats_published'] AS TIMESTAMP),
    nats_headers MAP<STRING, VARBINARY> DEFAULT CAST(CONNECTOR_METADATA()['nats_headers'] AS MAP<STRING, VARBINARY>)
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "name": "nats_with_metadata",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "All"
                },
                "include_subject": true,
                "include_stream_sequence": true,
                "include_published": true,
                "include_headers": true
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);
```

### Converting NATS header values to strings

NATS headers can contain arbitrary byte arrays, but in practice they typically hold UTF-8–encoded strings.
Use the `BIN2UTF8` function to convert binary values to text:

```sql
CREATE MATERIALIZED VIEW v AS
SELECT
  BIN2UTF8(nats_headers['my_header']) AS my_header
FROM messages_with_metadata;
```

## Additional resources

For more information, see:

* [Top-level connector documentation](/connectors/)
* [Fault tolerance](/pipelines/fault-tolerance)
* Data formats such as [JSON](/formats/json) and [CSV](/formats/csv)
* [NATS JetStream documentation](https://docs.nats.io/nats-concepts/jetstream)
* [NATS Ordered Consumer documentation](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#orderedconsumer)
