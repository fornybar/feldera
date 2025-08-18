#[cfg(test)]
mod tests {
    use feldera_types::transport::nats::{ConnectOptions, Auth, JetStreamConfig, NatsOutputConfig};
    use crate::transport::nats::NatsFtOutputEndpoint;
    use super::super::output::OutputPosition;
    use feldera_adapterlib::transport::OutputEndpoint;
    use anyhow::Result as AnyResult;
    use async_nats::{self, jetstream};
    use std::time::Duration;
    use tokio;

    // Re-use the NATS server utilities from the input tests
    mod util {
        use crate::test::wait;
        use anyhow::{anyhow, Result as AnyResult};
        use async_nats::Client;
        use serde::Deserialize;
        use std::env;
        use std::fs;
        use std::path::Path;
        use std::process::{Child, Command, Stdio};
        use std::time::{Duration, Instant};

        pub struct ProcessKillGuard {
            process: Child,
        }

        impl ProcessKillGuard {
            fn new(process: Child) -> Self {
                Self { process }
            }
        }

        impl Drop for ProcessKillGuard {
            fn drop(&mut self) {
                let _ = self.process.kill();
                let _ = self.process.wait();
            }
        }

        pub async fn wait_for_nats_ready(addr: &str, timeout: Duration) -> anyhow::Result<Client> {
            let deadline = Instant::now() + timeout;
            loop {
                match async_nats::connect(addr).await {
                    Ok(client) => return Ok(client),
                    Err(_) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(100)).await
                    }
                    Err(e) => return Err(anyhow::anyhow!("Timeout waiting for NATS: {e}")),
                }
            }
        }

        pub fn start_nats_and_get_address() -> AnyResult<(ProcessKillGuard, String)> {
            let nats_ip_addr = "127.0.0.1";
            const RANDOM_PORT: &str = "-1";

            let temp_dir = env::temp_dir();
            let port_file_dir = temp_dir.join("nats_ports");

            fs::create_dir_all(&port_file_dir)?;

            let child = Command::new("nats-server")
                .arg("-a")
                .arg(nats_ip_addr)
                .arg("-p")
                .arg(RANDOM_PORT)
                .arg("--ports_file_dir")
                .arg(port_file_dir.to_str().unwrap())
                .arg("--jetstream")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;

            let pid = child.id();
            let port_file_path = port_file_dir.join(format!("nats-server_{}.ports", pid));

            let child = ProcessKillGuard::new(child);

            if wait(|| port_file_path.exists(), 1000).is_err() {
                return Err(anyhow!("Port file was not created within timeout period"));
            }

            fn get_address_from_ports_file(file_path: &Path) -> AnyResult<String> {
                #[derive(Deserialize)]
                struct PortsData {
                    nats: Vec<String>,
                }

                let port_content = fs::read_to_string(&file_path)?;
                let ports_data: PortsData = serde_json::from_str(&port_content)
                    .map_err(|_| anyhow!("Could not parse ports file"))?;

                ports_data
                    .nats
                    .into_iter()
                    .next()
                    .ok_or(anyhow!("No NATS addresses found in port file"))
            }

            let nats_addr = get_address_from_ports_file(&port_file_path)?;

            Ok((child, nats_addr))
        }
    }

    fn create_ft_test_config(server_url: String, stream_name: String) -> NatsOutputConfig {
        NatsOutputConfig {
            connection_config: ConnectOptions {
                server_url,
                auth: Auth::default(),
            },
            subject: "test.ft.output".to_string(),
            headers: None,
            jetstream: Some(JetStreamConfig {
                stream_name,
                enable_fault_tolerance: true,
                max_age: None,
                max_bytes: None,
                max_messages: None,
            }),
        }
    }

    #[test]
    fn test_fault_tolerant_endpoint_creation() {
        let config = create_ft_test_config("nats://localhost:4222".to_string(), "test_stream".to_string());
        let endpoint = NatsFtOutputEndpoint::new(config);
        assert!(endpoint.is_ok());
        
        let endpoint = endpoint.unwrap();
        assert!(endpoint.is_fault_tolerant());
    }

    #[test]
    fn test_fault_tolerant_endpoint_requires_jetstream() {
        let mut config = create_ft_test_config("nats://localhost:4222".to_string(), "test_stream".to_string());
        config.jetstream = None;
        
        let endpoint = NatsFtOutputEndpoint::new(config);
        assert!(endpoint.is_err());
        assert!(endpoint.unwrap_err().to_string().contains("JetStream configuration required"));
    }

    #[test]
    fn test_fault_tolerant_endpoint_requires_ft_enabled() {
        let mut config = create_ft_test_config("nats://localhost:4222".to_string(), "test_stream".to_string());
        config.jetstream.as_mut().unwrap().enable_fault_tolerance = false;
        
        let endpoint = NatsFtOutputEndpoint::new(config);
        assert!(endpoint.is_err());
        assert!(endpoint.unwrap_err().to_string().contains("Fault tolerance must be enabled"));
    }

    #[test]
    fn test_output_position() {
        let mut pos = OutputPosition::new(42);
        assert_eq!(pos.step, 42);
        assert_eq!(pos.substep, 0);
        
        pos.next_substep();
        assert_eq!(pos.step, 42);
        assert_eq!(pos.substep, 1);
    }

    #[tokio::test]
    async fn test_ft_output_to_jetstream() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

        let stream_name = "test_ft_stream".to_string();
        let config = create_ft_test_config(nats_url, stream_name.clone());
        let mut endpoint = NatsFtOutputEndpoint::new(config)?;
        
        // Mock error callback
        let error_callback = |_fatal: bool, _error: anyhow::Error| {};
        endpoint.connect(Box::new(error_callback))?;

        // Create JetStream context to read messages
        let jetstream = jetstream::new(client);
        let stream = jetstream.get_stream(&stream_name).await?;

        // Send test data
        let test_data = b"Hello, NATS JetStream!";
        endpoint.batch_start(0)?;
        endpoint.push_buffer(test_data)?;
        endpoint.batch_end()?;

        // Verify message was stored in JetStream
        tokio::time::timeout(Duration::from_secs(5), async {
            let message = stream.get_last_raw_message_by_subject("test.ft.output").await?;
            assert_eq!(message.payload.as_ref(), test_data);
            assert_eq!(message.subject.as_str(), "test.ft.output");
            
            // Verify step tracking headers
            if let Some(step_header) = message.headers.get("Feldera-Step") {
                assert_eq!(std::str::from_utf8(step_header.as_ref())?, "0");
            } else {
                panic!("Expected Feldera-Step header not found");
            }

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for message"))?
        .map_err(|e| anyhow::anyhow!("Error reading message: {}", e))?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ft_output_with_headers() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

        let stream_name = "test_ft_headers_stream".to_string();
        let config = create_ft_test_config(nats_url, stream_name.clone());
        let mut endpoint = NatsFtOutputEndpoint::new(config)?;
        
        let error_callback = |_fatal: bool, _error: anyhow::Error| {};
        endpoint.connect(Box::new(error_callback))?;

        let jetstream = jetstream::new(client);
        let stream = jetstream.get_stream(&stream_name).await?;

        // Send key-value pair with headers
        endpoint.batch_start(0)?;
        endpoint.push_key(
            Some(b"test_key"), 
            Some(b"test_value"), 
            &[("Custom-Header", Some(b"custom_value"))]
        )?;
        endpoint.batch_end()?;

        // Verify message and headers
        tokio::time::timeout(Duration::from_secs(5), async {
            let message = stream.get_last_raw_message_by_subject("test.ft.output").await?;
            assert_eq!(message.payload.as_ref(), b"test_key:test_value");
            
            // Verify Feldera tracking headers
            assert!(message.headers.get("Feldera-Step").is_some());
            assert!(message.headers.get("Feldera-Substep").is_some());
            
            // Verify custom header
            if let Some(custom_header) = message.headers.get("Custom-Header") {
                assert_eq!(std::str::from_utf8(custom_header.as_ref())?, "custom_value");
            } else {
                panic!("Expected Custom-Header not found");
            }

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for message"))?
        .map_err(|e| anyhow::anyhow!("Error reading message: {}", e))?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ft_output_multiple_steps() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

        let stream_name = "test_ft_steps_stream".to_string();
        let config = create_ft_test_config(nats_url, stream_name.clone());
        let mut endpoint = NatsFtOutputEndpoint::new(config)?;
        
        let error_callback = |_fatal: bool, _error: anyhow::Error| {};
        endpoint.connect(Box::new(error_callback))?;

        let jetstream = jetstream::new(client);
        let stream = jetstream.get_stream(&stream_name).await?;

        // Send multiple steps with multiple messages each
        for step in 0..3 {
            endpoint.batch_start(step)?;
            endpoint.push_buffer(format!("step_{}_msg_1", step).as_bytes())?;
            endpoint.push_buffer(format!("step_{}_msg_2", step).as_bytes())?;
            endpoint.batch_end()?;
        }

        // Verify all messages were stored with correct step tracking
        tokio::time::timeout(Duration::from_secs(5), async {
            // Get stream info to check message count
            let info = stream.cached_info();
            assert!(info.state.messages >= 6); // At least 6 messages (3 steps * 2 messages each)

            // Check the last message has the correct step
            let last_message = stream.get_last_raw_message_by_subject("test.ft.output").await?;
            if let Some(step_header) = last_message.headers.get("Feldera-Step") {
                assert_eq!(std::str::from_utf8(step_header.as_ref())?, "2");
            } else {
                panic!("Expected Feldera-Step header not found");
            }

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for messages"))?
        .map_err(|e| anyhow::anyhow!("Error reading messages: {}", e))?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ft_output_restart_recovery() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

        let stream_name = "test_ft_recovery_stream".to_string();
        let config = create_ft_test_config(nats_url.clone(), stream_name.clone());

        // First endpoint - send some data
        {
            let mut endpoint1 = NatsFtOutputEndpoint::new(config.clone())?;
            let error_callback = |_fatal: bool, _error: anyhow::Error| {};
            endpoint1.connect(Box::new(error_callback))?;

            endpoint1.batch_start(0)?;
            endpoint1.push_buffer(b"first_endpoint_message")?;
            endpoint1.batch_end()?;

            endpoint1.batch_start(1)?;
            endpoint1.push_buffer(b"second_step_message")?;
            endpoint1.batch_end()?;
        } // Drop first endpoint

        // Second endpoint - should resume from step 2
        {
            let mut endpoint2 = NatsFtOutputEndpoint::new(config)?;
            let error_callback = |_fatal: bool, _error: anyhow::Error| {};
            endpoint2.connect(Box::new(error_callback))?;

            // This should work fine as it should resume from step 2
            endpoint2.batch_start(2)?;
            endpoint2.push_buffer(b"resumed_message")?;
            endpoint2.batch_end()?;
        }

        // Verify all messages are in the stream
        let jetstream = jetstream::new(client);
        let stream = jetstream.get_stream(&stream_name).await?;
        
        tokio::time::timeout(Duration::from_secs(5), async {
            let info = stream.cached_info();
            assert!(info.state.messages >= 3); // At least 3 messages from both endpoints

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for recovery test"))?
        .map_err(|e| anyhow::anyhow!("Error in recovery test: {}", e))?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ft_stream_auto_creation() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

        let stream_name = "auto_created_stream".to_string();
        let config = create_ft_test_config(nats_url, stream_name.clone());
        let mut endpoint = NatsFtOutputEndpoint::new(config)?;
        
        let error_callback = |_fatal: bool, _error: anyhow::Error| {};
        
        // The stream should be created automatically on connect
        endpoint.connect(Box::new(error_callback))?;

        // Verify the stream was created
        let jetstream = jetstream::new(client);
        let stream_result = jetstream.get_stream(&stream_name).await;
        assert!(stream_result.is_ok(), "Stream should have been auto-created");

        let stream = stream_result?;
        let info = stream.cached_info();
        assert_eq!(info.config.name, stream_name);
        assert!(info.config.subjects.contains(&"test.ft.output".to_string()));

        Ok(())
    }
}