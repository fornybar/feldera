use anyhow::{anyhow, Context, Error as AnyError, Result as AnyResult};
use async_nats::{self, HeaderMap, HeaderValue};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint, Step};
use feldera_types::transport::nats::NatsOutputConfig;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info_span, span::EnteredSpan};

use super::super::input::config_utils::translate_connect_options;


/// NATS output endpoint for publishing messages to NATS subjects.
pub struct NatsOutputEndpoint {
    config: Arc<NatsOutputConfig>,
    client: Option<async_nats::Client>,
    async_error_callback: Option<AsyncErrorCallback>,
}

impl NatsOutputEndpoint {
    pub fn new(config: NatsOutputConfig) -> AnyResult<Self> {
        Ok(Self {
            config: Arc::new(config),
            client: None,
            async_error_callback: None,
        })
    }

    pub fn span(&self) -> EnteredSpan {
        info_span!(
            "nats_output",
            ft = false,
            subject = self.config.subject,
            server = self.config.connection_config.server_url
        )
        .entered()
    }

    async fn connect_async(&mut self) -> AnyResult<()> {
        let _guard = self.span();

        let connect_options = translate_connect_options(&self.config.connection_config)
            .await.context("Failed to translate NATS connection options")?;

        let client = connect_options
            .connect(&self.config.connection_config.server_url)
            .await
            .context("Failed to connect to NATS server")?;

        self.client = Some(client);
        Ok(())
    }

    //fn build_headers(&self, headers: &[(&str, Option<&[u8]>)]) -> AnyResult<HeaderMap> {
    //    let mut header_map = HeaderMap::new();
    //
    //    // Add configured headers from config
    //    if let Some(config_headers) = &self.config.headers {
    //        for (key, value) in config_headers {
    //            let header_value = HeaderValue::from_str(value)
    //                .with_context(|| format!("Invalid header value for key '{}': '{}'", key, value))?;
    //            header_map.insert(key, header_value);
    //        }
    //    }
    //
    //    // Add headers from the push_key call
    //    for (key, value_opt) in headers {
    //        if let Some(value_bytes) = value_opt {
    //            let value_str = std::str::from_utf8(value_bytes)
    //                .with_context(|| format!("Header value for key '{}' is not valid UTF-8", key))?;
    //            let header_value = HeaderValue::from_str(value_str)
    //                .with_context(|| format!("Invalid header value for key '{}': '{}'", key, value_str))?;
    //            header_map.insert(*key, header_value);
    //        }
    //    }
    //
    //    Ok(header_map)
    //}
}

impl OutputEndpoint for NatsOutputEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let _guard = self.span();
        self.async_error_callback = Some(async_error_callback);

        // Use DBSP tokio runtime to connect
        TOKIO.block_on(self.connect_async())
            .context("Failed to establish NATS connection")?;

        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        // NATS has a default max message size of 1MB, but this can be configured
        // on the server. We use a conservative default here.
        1_000_000
    }

    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
        // NATS doesn't require explicit batch handling for non-fault-tolerant mode
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();

        let client = self.client.as_ref()
            .ok_or_else(|| anyhow!("NATS client not connected"))?;

        let subject = &self.config.subject;
        let payload = bytes::Bytes::from(Vec::from(buffer));

        // Publish the message synchronously using DBSP tokio runtime
        TOKIO.block_on(async {
            //if let Some(config_headers) = &self.config.headers {
            //    if !config_headers.is_empty() {
            //        let headers = self.build_headers(&[])?;
            //        client.publish_with_headers(subject.clone(), headers, payload)
            //            .await
            //            .context("Failed to publish message with headers to NATS")?;
            //    } else {
            //        client.publish(subject.clone(), payload)
            //            .await
            //            .context("Failed to publish message to NATS")?;
            //    }
            //} else {
                client.publish(subject.clone(), payload)
                    .await
                    .context("Failed to publish message to NATS")?;
            //}
            Ok::<(), AnyError>(())
        })?;

        Ok(())
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = self.span();

        let client = self.client.as_ref()
            .ok_or_else(|| anyhow!("NATS client not connected"))?;

        let subject = &self.config.subject;

        // For NATS, we'll encode key-value pairs as a simple format
        // This could be enhanced to support more sophisticated encoding schemes
        let payload = match (key, val) {
            (Some(k), Some(v)) => {
                // Simple key:value format
                let key_str = std::str::from_utf8(k)
                    .context("Key is not valid UTF-8")?;
                let val_str = std::str::from_utf8(v)
                    .context("Value is not valid UTF-8")?;
                format!("{}:{}", key_str, val_str).into_bytes()
            }
            (Some(k), None) => {
                // Key only (deletion marker)
                let key_str = std::str::from_utf8(k)
                    .context("Key is not valid UTF-8")?;
                format!("{}:", key_str).into_bytes()
            }
            (None, Some(v)) => {
                // Value only
                v.to_vec()
            }
            (None, None) => {
                return Err(anyhow!("Both key and value cannot be None"));
            }
        };

        TOKIO.block_on(async {
            //let header_map = self.build_headers(headers)?;
            //if header_map.is_empty() {
                client.publish(subject.clone(), payload.into())
                    .await
                    .context("Failed to publish key-value message to NATS")?;
            //} else {
            //    client.publish_with_headers(subject.clone(), header_map, payload.into())
            //        .await
            //        .context("Failed to publish key-value message with headers to NATS")?;
            //}
            Ok::<(), AnyError>(())
        })?;

        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        // NATS doesn't require explicit batch handling for non-fault-tolerant mode
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        // This implementation doesn't support fault tolerance yet
        // Could be enhanced to support NATS JetStream for exactly-once delivery
        false
    }
}

impl Drop for NatsOutputEndpoint {
    fn drop(&mut self) {
        // The async_nats client handles cleanup automatically
    }
}
