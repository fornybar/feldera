use anyhow::{anyhow, bail, Context, Result as AnyResult};
use async_nats::{self, jetstream, HeaderMap, HeaderValue};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint, Step};
use feldera_types::transport::nats::NatsOutputConfig;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, info_span, span::EnteredSpan, warn};

use super::super::input::config_utils::translate_connect_options;


/// State of the fault-tolerant NATS output endpoint.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum FtState {
    /// Just created.
    New,
    /// `connect` has been called.
    Connected,
    /// `batch_start_step()` has been called.  The next call to `push_buffer()`
    /// will write at position `.0`.
    BatchOpen(OutputPosition),
    /// `batch_end` has been called for step `.0`.
    BatchClosed(Step),
}

/// A position in the JetStream output.
/// 
/// This tracks both the step and substep within that step for precise ordering.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OutputPosition {
    /// The step number.
    step: Step,
    /// An index within the step. The first message output in a step has
    /// substep 0, the second has substep 1, and so on.
    substep: u64,
}

impl OutputPosition {
    fn new(step: Step) -> Self {
        Self { step, substep: 0 }
    }
    
    fn next_substep(&mut self) {
        self.substep += 1;
    }
}

/// NATS fault-tolerant output endpoint using JetStream for durability.
pub struct NatsFtOutputEndpoint {
    config: Arc<NatsOutputConfig>,
    client: Option<async_nats::Client>,
    jetstream: Option<jetstream::Context>,
    async_error_callback: Option<AsyncErrorCallback>,
    state: FtState,
    next_step: Step,
    buffered_messages: Vec<(OutputPosition, Vec<u8>, Option<HeaderMap>)>,
}

impl NatsFtOutputEndpoint {
    pub fn new(config: NatsOutputConfig) -> AnyResult<Self> {
        // Validate that JetStream is configured for fault tolerance
        let jetstream_config = config.jetstream.as_ref()
            .ok_or_else(|| anyhow!("JetStream configuration required for fault-tolerant NATS output"))?;
        
        if !jetstream_config.enable_fault_tolerance {
            bail!("Fault tolerance must be enabled in JetStream configuration");
        }

        Ok(Self {
            config: Arc::new(config),
            client: None,
            jetstream: None,
            async_error_callback: None,
            state: FtState::New,
            next_step: 0,
            buffered_messages: Vec::new(),
        })
    }

    pub fn span(&self) -> EnteredSpan {
        let jetstream_config = self.config.jetstream.as_ref().unwrap();
        info_span!(
            "nats_ft_output",
            ft = true,
            subject = self.config.subject,
            stream = jetstream_config.stream_name,
            server = self.config.connection_config.server_url
        )
        .entered()
    }

    async fn connect_async(&mut self) -> AnyResult<()> {
        let _guard = self.span();

        let connect_options = translate_connect_options(&self.config.connection_config)
            .await
            .context("Failed to translate NATS connection options")?;

        let client = connect_options
            .connect(&self.config.connection_config.server_url)
            .await
            .context("Failed to connect to NATS server")?;

        // Create JetStream context
        let jetstream = jetstream::new(client.clone());

        // Ensure the stream exists
        self.ensure_stream_exists(&jetstream).await
            .context("Failed to ensure JetStream stream exists")?;

        // Find the resume position by querying the stream
        let resume_step = self.find_resume_position(&jetstream).await
            .context("Failed to determine resume position")?;

        info!("Resuming from step: {}", resume_step);

        self.client = Some(client);
        self.jetstream = Some(jetstream);
        self.next_step = resume_step;

        Ok(())
    }

    async fn ensure_stream_exists(&self, jetstream: &jetstream::Context) -> AnyResult<()> {
        let jetstream_config = self.config.jetstream.as_ref().unwrap();
        let stream_name = &jetstream_config.stream_name;

        // Try to get the stream info first
        match jetstream.get_stream(stream_name).await {
            Ok(_) => {
                debug!("JetStream stream '{}' already exists", stream_name);
                return Ok(());
            }
            Err(_) => {
                // Stream doesn't exist, we'll create it
                debug!("Stream doesn't exist, will create it");
            }
        }

        // Create stream configuration
        let mut stream_config = jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![self.config.subject.clone()],
            ..Default::default()
        };

        // Apply optional configuration
        if let Some(max_age) = jetstream_config.max_age {
            stream_config.max_age = max_age;
        }
        if let Some(max_bytes) = jetstream_config.max_bytes {
            stream_config.max_bytes = max_bytes;
        }
        if let Some(max_messages) = jetstream_config.max_messages {
            stream_config.max_messages = max_messages;
        }

        // Create the stream
        jetstream
            .create_stream(stream_config)
            .await
            .context("Failed to create JetStream stream")?;

        info!("Created JetStream stream '{}'", stream_name);
        Ok(())
    }

    async fn find_resume_position(&self, jetstream: &jetstream::Context) -> AnyResult<Step> {
        let jetstream_config = self.config.jetstream.as_ref().unwrap();
        let stream_name = &jetstream_config.stream_name;

        // Get the stream to query messages
        let stream = jetstream.get_stream(stream_name).await
            .context("Failed to get JetStream stream")?;

        // Get the last message to determine the latest step
        match stream.get_last_raw_message_by_subject(&self.config.subject).await {
            Ok(message) => {
                // Extract step information from message headers  
                if let Some(step_header) = message.headers.get("Feldera-Step") {
                    if let Ok(step_str) = std::str::from_utf8(step_header.as_ref()) {
                        if let Ok(step) = step_str.parse::<Step>() {
                            // Resume from the next step
                            return Ok(step + 1);
                        }
                    }
                }
                // If we can't parse the step, start from 0
                warn!("Could not parse step from last message, starting from step 0");
                Ok(0)
            }
            Err(_) => {
                // No messages in stream or other error, start from step 0
                debug!("No messages in stream or error retrieving last message, starting from step 0");
                Ok(0)
            }
        }
    }

    fn build_headers(&self, extra_headers: &[(&str, Option<&[u8]>)], position: &OutputPosition) -> AnyResult<HeaderMap> {
        let mut header_map = HeaderMap::new();

        // Add Feldera-specific headers for tracking
        header_map.insert(
            "Feldera-Step",
            HeaderValue::from(position.step.to_string()),
        );
        header_map.insert(
            "Feldera-Substep", 
            HeaderValue::from(position.substep.to_string()),
        );

        // Add configured headers from config
        if let Some(config_headers) = &self.config.headers {
            for (key, value) in config_headers {
                let header_value = HeaderValue::from(value.clone());
                header_map.insert(key.clone(), header_value);
            }
        }

        // Add headers from the push_key call
        for (key, value_opt) in extra_headers {
            if let Some(value_bytes) = value_opt {
                let value_str = std::str::from_utf8(value_bytes)
                    .with_context(|| format!("Header value for key '{}' is not valid UTF-8", key))?;
                let header_value = HeaderValue::from(value_str.to_string());
                header_map.insert(key.to_string(), header_value);
            }
        }

        Ok(header_map)
    }

    async fn flush_buffered_messages(&mut self) -> AnyResult<()> {
        let _guard = self.span();

        if self.buffered_messages.is_empty() {
            return Ok(());
        }

        let jetstream = self.jetstream.as_ref()
            .ok_or_else(|| anyhow!("JetStream not connected"))?;

        debug!("Flushing {} buffered messages", self.buffered_messages.len());

        // Publish all buffered messages
        for (position, payload, headers) in &self.buffered_messages {
            let subject = &self.config.subject;
            let payload_bytes = bytes::Bytes::from(payload.clone());

            let publish_ack = if let Some(headers) = headers {
                jetstream
                    .publish_with_headers(subject.clone(), headers.clone(), payload_bytes)
                    .await
                    .context("Failed to publish message with headers to JetStream")?
            } else {
                jetstream
                    .publish(subject.clone(), payload_bytes)
                    .await
                    .context("Failed to publish message to JetStream")?
            };

            // Wait for acknowledgment to ensure message is stored
            publish_ack.await
                .context("Failed to get acknowledgment from JetStream")?;

            debug!("Published message for step {} substep {}", position.step, position.substep);
        }

        self.buffered_messages.clear();
        info!("Successfully flushed all buffered messages");
        Ok(())
    }
}

impl OutputEndpoint for NatsFtOutputEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let _guard = self.span();
        self.async_error_callback = Some(async_error_callback);

        // Use DBSP tokio runtime to connect
        TOKIO.block_on(self.connect_async())
            .context("Failed to establish NATS JetStream connection")?;

        self.state = FtState::Connected;
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        // JetStream has configurable max message size, use conservative default
        1_000_000
    }

    fn batch_start(&mut self, step: Step) -> AnyResult<()> {
        let _guard = self.span();
        
        match self.state {
            FtState::New => {
                return Err(anyhow!("connect() must be called before batch_start()"));
            }
            FtState::Connected => {
                // First batch
                if step != self.next_step {
                    return Err(anyhow!(
                        "Expected step {}, got step {}",
                        self.next_step,
                        step
                    ));
                }
            }
            FtState::BatchClosed(closed_step) => {
                if step != closed_step + 1 {
                    return Err(anyhow!(
                        "Expected step {}, got step {} (last closed: {})",
                        closed_step + 1,
                        step,
                        closed_step
                    ));
                }
            }
            FtState::BatchOpen(_) => {
                return Err(anyhow!("batch_end() must be called before starting a new batch"));
            }
        }

        let position = OutputPosition::new(step);
        self.state = FtState::BatchOpen(position);
        self.buffered_messages.clear();

        debug!("Started batch for step {}", step);
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();

        let mut position = match self.state {
            FtState::BatchOpen(pos) => pos,
            _ => return Err(anyhow!("batch_start() must be called before push_buffer()")),
        };

        // Buffer the message for later publishing
        self.buffered_messages.push((position, buffer.to_vec(), None));

        // Update position for next message
        position.next_substep();
        self.state = FtState::BatchOpen(position);

        Ok(())
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = self.span();

        let mut position = match self.state {
            FtState::BatchOpen(pos) => pos,
            _ => return Err(anyhow!("batch_start() must be called before push_key()")),
        };

        // For NATS, encode key-value pairs as a simple format
        let payload = match (key, val) {
            (Some(k), Some(v)) => {
                let key_str = std::str::from_utf8(k).context("Key is not valid UTF-8")?;
                let val_str = std::str::from_utf8(v).context("Value is not valid UTF-8")?;
                format!("{}:{}", key_str, val_str).into_bytes()
            }
            (Some(k), None) => {
                let key_str = std::str::from_utf8(k).context("Key is not valid UTF-8")?;
                format!("{}:", key_str).into_bytes()
            }
            (None, Some(v)) => v.to_vec(),
            (None, None) => {
                return Err(anyhow!("Both key and value cannot be None"));
            }
        };

        // Build headers including position tracking
        let header_map = self.build_headers(headers, &position)?;

        // Buffer the message for later publishing
        self.buffered_messages.push((position, payload, Some(header_map)));

        // Update position for next message
        position.next_substep();
        self.state = FtState::BatchOpen(position);

        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        let _guard = self.span();

        let step = match self.state {
            FtState::BatchOpen(pos) => pos.step,
            _ => return Err(anyhow!("batch_start() must be called before batch_end()")),
        };

        // Flush all buffered messages to JetStream
        TOKIO.block_on(self.flush_buffered_messages())
            .context("Failed to flush buffered messages to JetStream")?;

        self.state = FtState::BatchClosed(step);
        self.next_step = step + 1;

        debug!("Completed batch for step {}", step);
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        true
    }
}

impl Drop for NatsFtOutputEndpoint {
    fn drop(&mut self) {
        // The async_nats client handles cleanup automatically
    }
}

pub fn span(subject: &str, stream: &str) -> EnteredSpan {
    info_span!(
        "nats_ft_output", 
        ft = true, 
        subject = String::from(subject),
        stream = String::from(stream)
    ).entered()
}