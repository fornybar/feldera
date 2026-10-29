# NATS input connector

Feldera can consume a stream of changes to a SQL table from NATS JetStream
with the `nats_input` connector.

The NATS input connector supports exactly-once [fault
tolerance](/pipelines/fault-tolerance).

## How it works

- **Reading**: the connector reads with an ephemeral, single-replica,
  in-memory pull consumer without acknowledgments, and tracks its position in
  the stream (the sequence of the next message it wants) itself. Every message
  carries a delivery number, so the connector notices a message that the
  server sent but the connection lost. On such a gap, on a deleted consumer,
  on missed heartbeats, or on any other read error, it replaces the consumer
  with a new one at its own position.
- **Retry loop with health checks**: on transient failures, the connector
  enters retry mode and reconnects with exponential backoff. While no message
  arrives, it checks every `inactivity_timeout` that the stream (still the same
  one) and its consumer exist.
- **Exactly-once semantics**: each pipeline step records the range of stream
  sequences it ingested, the number of messages, and the stream's identity.
  After a crash, the pipeline replays the steps after its last checkpoint by
  reading exactly those messages back by sequence (see
  [Replay](#replay)), then continues from where they end.

### Replay

Replay reads a step's messages by stream sequence, without creating a
consumer:

- With **batched Direct Get** (nats-server 2.11 or later, on streams with
  `allow_direct`), each request returns up to 512 messages of a subject from
  any replica of the stream. The connector checks that the answer is complete
  (every message names the one before it) and comes from the stream itself.
- On older servers, and on streams without `allow_direct`, it reads the
  messages from the stream leader with `STREAM.MSG.GET`, one request per
  message, which is slower. The connector accepts such streams, but logs a
  warning once each time it opens that names the reason: the nats-server
  version (shown as reported, including a version it cannot parse), or
  `allow_direct` being off. `nats stream edit <stream> --allow-direct` turns
  it on.
- When a Direct Get request fails, or its answer is incomplete or does not
  match the step (a replica may lag behind the leader), the connector reads
  the step again from the stream leader. The metric
  `input_connector_nats_replay_leader_fallbacks_total` counts only these
  fallbacks, not the steps that are read from the leader from the start.
  Steps of a `LastPerSubject` snapshot are read one message at a time, and
  their fallbacks are not counted.

The connector fetches and checks every message of a step before it feeds any
of them to the pipeline, so a replay that fails or is retried never ingests
part of a step twice. A step whose messages are no longer all in the stream
fails the replay with a fatal error.

### Recommended stream configuration

- `retention: limits` (the default). With `interest` or `workqueue`
  retention, the server may discard a message as soon as the connector has
  read it, before a checkpoint covers it, and then the pipeline cannot replay
  it after a crash. The connector logs a warning for such streams.
- `allow_direct: true` (the default of `nats stream add`; turn it on for an
  existing stream with `nats stream edit <stream> --allow-direct`) on
  nats-server 2.11 or later, so that replay can use batched Direct Get.
  Without it, the connector still works, but replay reads each message from
  the stream leader with one request per message, which is slower, and the
  connector logs a warning when it opens (see [Replay](#replay)).
- Keep messages (`max_age`, `max_msgs`, `max_bytes`) for longer than the
  pipeline can take between checkpoints, plus the time it may be stopped.

## NATS Input Connector Configuration

The connector configuration consists of three main sections:

### Connection Options

| Property                | Type   | Required | Description |
|------------------------|--------|----------|-------------|
| `server_url`           | string | Yes      | NATS server URL (e.g., `nats://localhost:4222`) |
| `auth`                 | object | No       | Authentication configuration (see [Authentication](#authentication)) |
| `tls`                  | object | No       | TLS configuration (see [TLS](#tls)) |
| `connection_timeout` | duration | No | Connection timeout, for example `10s`. How long to wait when establishing the initial connection to the NATS server. Replaces the deprecated `connection_timeout_secs`. Default: `10s` |
| `request_timeout` | duration | No | Request timeout, for example `10s`. How long to wait for responses to requests. Replaces the deprecated `request_timeout_secs`. Default: `10s` |

### Stream Configuration

| Property      | Type   | Required | Description |
|--------------|--------|----------|-------------|
| `stream_name` | string | Yes      | The name of the NATS JetStream stream to consume from |
| `inactivity_timeout` | duration | No | Maximum idle time while waiting for the next message before running a stream/server health check, for example `60s`. Must be at least one second. Replaces the deprecated `inactivity_timeout_secs`. Default: `60s` |
| `retry_interval` | duration | No | Delay before the first automatic retry attempt in retry mode, for example `5s`. The delay doubles after each consecutive failure. Must be at least one second. Replaces the deprecated `retry_interval`. Default: `5s` |
| `retry_max_interval` | duration | No | Upper bound on the delay between retry attempts, for example `5m`. Must be at least `retry_interval`. Set it equal to `retry_interval` to retry at a fixed interval. Default: `5m` |
| `retry_max_attempts` | integer | No | Number of consecutive failed retry attempts after which the connector gives up with a fatal error. Must be at least 1. Default: unset (retry forever) |

### Consumer Configuration

| Property           | Type                    | Required | Description |
|-------------------|-------------------------|----------|-------------|
| `name`            | string                  | No       | Prefix for the names of the consumers the connector creates (see [Consumer lifecycle](#consumer-lifecycle)). Default: the table name |
| `description`     | string                  | No       | Consumer description |
| `filter_subjects` | string list             | No       | Filter messages by subject(s). If empty, consumes all subjects in the stream. The subjects may not overlap |
| `replay_policy`   | variant                 | No       | Message replay speed: `"Instant"` (default, fast) or `"Original"` (rate-limited at original timing) |
| `rate_limit`      | integer                 | No       | Not supported: NATS limits the rate only of push consumers, and the connector reads with a pull consumer. A value other than 0 is rejected when the connector opens; remove `rate_limit` |
| `deliver_policy`  | variant                 | Yes      | Starting point for reading from the stream (see [Deliver Policy](#deliver-policy)) |
| `max_waiting`     | integer                 | No       | Maximum outstanding pull requests. Default: 0 |
| `metadata`        | map (string → string)   | No       | Consumer metadata key-value pairs |
| `max_batch`       | integer                 | No       | Maximum messages per batch |
| `max_bytes`       | integer                 | No       | Maximum bytes per batch |
| `max_expiry` | duration | No | Maximum time a pull request stays parked on the server, for example `30s`. Replaces the deprecated `max_expires`. |

#### Deliver Policy

The `deliver_policy` field determines where in the stream to start consuming messages:

- `"All"` - Start from the earliest available message in the stream
- `"Last"` - Start from the last message in the stream
- `"New"` - Start from new messages only (messages arriving after the connector first starts)
- `"LastPerSubject"` - Start with the last message of each subject (useful for KV-like workloads), then read every newer message. Requires `filter_subjects`, because NATS delivers the last message per subject only to a consumer with a filter subject; without it, the connector is rejected when it opens. Use `[">"]` for all subjects. If the connector has to recreate its consumer before it has read the whole snapshot, it takes a new snapshot
- `{"ByStartSequence": {"start_sequence": 100}}` - Start from a specific sequence number
- `{"ByStartTime": {"start_time": "2024-01-01T12:00:00Z"}}` - Start from messages at or after the specified timestamp (RFC 3339 format)

The deliver policy chooses where the connector starts the first time. The
connector then remembers the position the server chose, so that after a pause,
an error, or a restart it continues from there; for example, `"New"` does not
skip what was published while the connector was paused.

#### Replay Policy

The `replay_policy` field controls how fast messages are delivered to the consumer:

- `"Instant"` (default) - Delivers messages as quickly as possible. Use for maximum throughput in production workloads.
- `"Original"` - Delivers messages at the rate they were originally received, preserving the timing between messages. Useful for:
  - Replaying production traffic patterns in test/staging environments
  - Load testing with realistic timing
  - Debugging scenarios where message timing matters

If not specified, defaults to `"Instant"`.

## Retry, startup and replay behavior

The connector distinguishes between **retryable** and **fatal** errors:

- **Retryable errors** (temporary network/server issues, missing stream during startup, transient message-stream failures, and temporary failures while fetching JetStream stream metadata used during startup, resume, or replay validation) move the connector into retry mode. It reports non-fatal endpoint errors and retries automatically with backoff (see [Retry backoff](#retry-backoff)).
- **Fatal errors** stop the connector and report a fatal endpoint error. This is used when checkpoint/replay metadata is incompatible with the current stream, and for configuration that no server can accept.

Configuration mistakes (an invalid stream name or filter subject, `rate_limit`,
`LastPerSubject` without `filter_subjects`, conflicting authentication
settings) fail the connector when it opens.

Before reading after startup or resume, the connector checks that the stream
is the one it read before and that its position is still in the stream's
available sequence range. During replay, it checks that the step's messages
are all still in the stream.

Only transient I/O failures during these validation checks are retried. Once the connector successfully reads the stream metadata, logical validation failures remain fatal.

Typical fatal scenarios include:

- **Stream deleted and recreated, or restored from an older backup**: a
  recreated stream numbers its messages from 1 again, and a stream restored
  from a backup taken before the pipeline's checkpoint has lost messages that
  the pipeline ingested; new messages then reuse their sequence numbers. The
  connector records the sequence and storage time of the last message it
  ingested, and the creation time of the stream. It fails with "is not the
  stream this pipeline read before" when the stream holds another message at
  that sequence, or, if the stream no longer has that sequence at all, when
  the creation time differs.

A stream restored from a backup of the same messages, or a later backup, is
the same stream to the connector: it resumes and replays from it, even though
the restore can give the stream a new creation time (nats-server 2.15 does,
as do clustered restores on some earlier versions). If retention has also
removed the last message the pipeline ingested, the connector cannot tell a
restored stream from a new one and fails.
- **Unread messages removed**: the stream no longer has messages that the
  pipeline has not read yet (a retention limit, a purge, or a delete removed
  them), so continuing would silently skip them. Removing messages that the
  pipeline has already read and checkpointed is fine, even if that leaves the
  stream empty.
- **Journaled messages removed**: a step after the last checkpoint must be
  replayed, but some of its messages are gone.

A connector that reads only some subjects (`filter_subjects`) cannot tell
whether removed messages were on its subjects. While it runs and waits for
messages, it moves its position past the messages of other subjects, so
that retention removing them is not mistaken for data loss.

In all of these cases the connector fails fast with a fatal error instead of retrying
forever, because the data needed to maintain exactly-once guarantees is permanently gone.

:::tip Recovery from fatal errors
To recover from a fatal error caused by stream data loss, you typically need to
reset the pipeline's checkpoint state (e.g., by recreating the pipeline) so it
starts fresh without referencing the now-invalid sequence numbers.
:::

### Retry backoff

Each retry attempt opens a new connection to the NATS server and creates a new
JetStream consumer. Creating a consumer at a checkpointed position makes the
server scan the stream to compute the consumer's pending count, which is
expensive on large streams. If many connectors retried at a short, fixed
interval against a struggling cluster, they would add load exactly when the
cluster can least afford it.

The connector therefore backs off exponentially. The first retry waits
`retry_interval`, and each consecutive failure doubles the wait, up to
`retry_max_interval`. Every wait below the maximum is lengthened by a
random amount of up to 25%, so that connectors that failed together (for
example, after a NATS node restart) do not retry in lockstep. The randomization
never shortens a wait below `retry_interval` or lengthens it beyond
`retry_max_interval`. With the defaults, the nominal waits are 5s, 10s,
20s, 40s, 80s, 160s, and then 300s for every later attempt.
A successful reconnect resets the backoff.

By default the connector retries forever. Set `retry_max_attempts` to stop
after that many consecutive failed retries with a fatal error, which leaves the
decision to resume to an operator. The same limit applies to retries during
replay.

**Upgrading:** before this change, the connector retried forever at a fixed
`retry_interval` (5s by default). A pipeline that relied on fast,
unbounded retries to recover quickly from brief NATS outages will now wait up
to `retry_max_interval` (`5m` by default) between attempts once the
backoff has grown that far. If your pipeline depends on recovering within a
few seconds, set `retry_max_interval` equal to `retry_interval` to
keep the old fixed-interval behavior.

### Consumer lifecycle

The connector creates a new ephemeral JetStream consumer when it starts,
resumes, or retries. Replay creates no consumer. Each consumer is named
`<prefix>_<uuid>`, where the prefix is the configured `consumer_config.name`,
or the table name if none is set. Characters that JetStream does not allow in
consumer names are replaced with `_`. The unique suffix prevents "consumer
already exists" errors on quick restarts, and the prefix identifies which
connector owns a consumer when you inspect the stream with `nats consumer ls`.
For example, to list the consumers of a connector whose prefix is `orders`:

```bash
nats consumer ls <stream> --names | grep '^orders_'
```

The connector deletes its consumer when it pauses, stops, or abandons a failed
reader before retrying. It also deletes the consumer after a create request
that timed out, because an overloaded server may still create the consumer
after the client has given up. Deletion is best-effort: if the server cannot be
reached, the consumer expires on its own after 60 seconds of inactivity.

Starting a reader (connecting, checking the stream, and creating the consumer)
runs in the background: a slow or unreachable server does not hold up the
pipeline's steps, and pausing or stopping the connector does not wait for it.

### Metrics

The connector exports the following metrics on the pipeline's metrics
endpoint, in addition to the standard input connector metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `input_connector_nats_consumers_created_total` | counter | JetStream consumers created (start, resume, and every retry) |
| `input_connector_nats_consumers_deleted_total` | counter | JetStream consumers explicitly deleted |
| `input_connector_nats_consumer_delete_failures_total` | counter | Delete requests that errored or timed out; the consumer is left to expire on its own |
| `input_connector_nats_retries_total` | counter | Reconnect attempts made in retry mode |
| `input_connector_nats_consecutive_failures` | gauge | Consecutive failed attempts in the current retry episode; 0 when healthy |
| `input_connector_nats_retry_state` | gauge | `0` healthy or paused, `1` retrying, `2` stopped after a fatal error |
| `input_connector_nats_replay_leader_fallbacks_total` | counter | Replayed steps read from the stream leader because a Direct Get answer was incomplete or did not match the step |

A steadily rising `input_connector_nats_consumers_created_total` while the
connector ingests no records signals that it is stuck reconnecting.

## Authentication

The NATS connector supports the standard NATS authentication methods through
the `auth` object. Configure exactly one method: `credentials`, `jwt` (with
`nkey`), `nkey`, `token`, or `user_and_password`. Configuring more than one
is rejected.

### Permissions

The connector makes these requests. Each `<stream>` is the configured
`stream_name`; each `<consumer>` is the name of one of the connector's own
consumers (see [Consumer lifecycle](#consumer-lifecycle)).

| Publish to | Used for |
|------------|----------|
| `$JS.API.STREAM.INFO.<stream>` | Checking the stream (identity, available sequences) before reading, during health checks, and before each replayed step |
| `$JS.API.CONSUMER.CREATE.<stream>.<consumer>` | Creating the pull consumer it reads with |
| `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` | Pulling messages |
| `$JS.API.CONSUMER.INFO.<stream>.<consumer>` | Health checks while no message arrives, and checks after a reconnect or a consumer create that timed out |
| `$JS.API.CONSUMER.DELETE.<stream>.<consumer>` | Deleting its consumers |
| `$JS.API.STREAM.MSG.GET.<stream>` | Replay from the stream leader, and moving the position past messages it does not read while idle |
| `$JS.API.DIRECT.GET.<stream>` | Replay with Direct Get |

Replies, pulled messages, and Direct Get answers arrive on the client's inbox,
so the user also needs permission to subscribe to `_INBOX.>`. The connector
always uses the default inbox prefix `_INBOX`. It does not acknowledge
messages and does not use `$JS.API.INFO`.

Consumer names are `<prefix>_<uuid>`, one token, so the consumer subjects need
the `*` wildcard. NATS wildcards match whole tokens only, so they cannot be
narrowed to the connector's prefix: the user can manage any consumer of the
stream.

For example, for a stream `orders`:

```text
authorization {
  users = [
    {
      user: feldera
      password: <password>
      permissions: {
        publish: {
          allow: [
            "$JS.API.STREAM.INFO.orders",
            "$JS.API.STREAM.MSG.GET.orders",
            "$JS.API.DIRECT.GET.orders",
            "$JS.API.CONSUMER.CREATE.orders.*",
            "$JS.API.CONSUMER.INFO.orders.*",
            "$JS.API.CONSUMER.DELETE.orders.*",
            "$JS.API.CONSUMER.MSG.NEXT.orders.*"
          ]
        }
        subscribe: {
          allow: ["_INBOX.>"]
        }
      }
    }
  ]
}
```

Without `$JS.API.DIRECT.GET.<stream>`, replay still works, but every replayed
step first waits for its Direct Get request to fail and is then read from the
stream leader. Without `$JS.API.STREAM.MSG.GET.<stream>`, a replay that cannot
use Direct Get fails and is retried, and a connector with `filter_subjects`
cannot move its position past other subjects' messages.

### Credentials File Authentication

Use a credentials file containing JWT and NKey seed:

```json
{
  "auth": {
    "credentials": {
      "FromFile": "/path/to/credentials.creds"
    }
  }
}
```

Or provide credentials directly as a string:

```json
{
  "auth": {
    "credentials": {
      "FromString": "-----BEGIN NATS USER JWT-----\n...\n------END NATS USER JWT------\n\n************************* IMPORTANT *************************\n..."
    }
  }
}
```

### JWT Authentication

For decentralized (operator-mode) authentication with the user JWT and NKey
seed stored separately (e.g. as two secrets) rather than as one `.creds`
file. The seed signs the connection nonce:

```json
{
  "auth": {
    "jwt": "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...",
    "nkey": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
  }
}
```

### NKey Authentication

For a bare NKey user declared in the server configuration (`nkey:` user):
the server issues a nonce and the connector signs it with the seed.

```json
{
  "auth": {
    "nkey": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
  }
}
```

### Token Authentication

```json
{
  "auth": {
    "token": "s3cret"
  }
}
```

### Username and Password Authentication

```json
{
  "auth": {
    "user_and_password": {
      "user": "myuser",
      "password": "mypassword"
    }
  }
}
```

:::tip
For production environments, it is strongly recommended to use [secret references](/connectors/secret-references) instead of hardcoding credentials in the configuration.
:::

## TLS

TLS options are configured through the `tls` object:

| Property                  | Type    | Required | Description |
|---------------------------|---------|----------|-------------|
| `require_tls`             | boolean | No       | Require an encrypted connection; refuse to connect to servers that do not offer TLS. Default: `false` |
| `root_certificates_file`  | string  | No       | Path to a PEM file with additional root certificates to trust when verifying the server certificate, for servers whose certificates are not signed by a public CA |

```json
{
  "connection_config": {
    "server_url": "tls://nats.example.com:4222",
    "tls": {
      "require_tls": true,
      "root_certificates_file": "/etc/nats-ca/ca.crt"
    }
  }
}
```

## Setting up NATS JetStream

Before using the NATS input connector, you need a NATS server with JetStream enabled and a stream created.

### Quickstart
The quickest way to start experimenting with Feldera and NATS is to use Docker Compose:

```bash
curl -L 'https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml' -o docker-compose.yml
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
    TEXT STRING
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

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```

### Only receive new NATS messages

If you only want to receive messages published after the Feldera pipeline starts,
change `deliver_policy` to `New`.

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    TEXT STRING
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

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```

### Filtering by subject

Use `filter_subjects` to only consume messages from specific subjects `text.area.2` and `text.*.3`:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    TEXT STRING
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

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```

### Replaying at original timing

You can use `"Original"` replay policy to replay production traffic in a test environment with realistic timing:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    TEXT STRING
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

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```
## Pipeline metadata

When the pipeline configuration includes a `given_name` field, Feldera
automatically sets `metadata["pipeline"]` on the NATS JetStream consumer to the
value of `given_name`. If you already set `metadata.pipeline` in your connector
configuration, your value takes precedence and is not overwritten.

For example, a pipeline named `"my_pipeline"` will produce a consumer whose
metadata contains `{"pipeline": "my_pipeline"}`:

```sql
CREATE TABLE events (
    id BIGINT,
    payload STRING
) WITH (
    'connectors' = '[{
        "name": "nats_in",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_stream",
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
```

This is useful for correlating NATS consumer metrics with Feldera pipeline
status in monitoring systems. For example, with
[`prometheus-nats-exporter`](https://github.com/nats-io/prometheus-nats-exporter),
use the `-jsz_consumer_meta_keys=pipeline` flag to surface the pipeline name as
a Prometheus label on all `nats_consumer_*` metrics. This allows alert rules
that join on the `pipeline` label with `feldera_pipeline_status`.

## Additional resources

For more information, see:

* [Top-level connector documentation](/connectors/)
* [Fault tolerance](/pipelines/fault-tolerance)
* Data formats such as [JSON](/formats/json) and [CSV](/formats/csv)
* [NATS JetStream documentation](https://docs.nats.io/nats-concepts/jetstream)
* [NATS Ordered Consumer documentation](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#orderedconsumer)
