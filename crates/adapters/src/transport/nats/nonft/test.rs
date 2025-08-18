#[cfg(test)]
mod tests {
    use feldera_types::transport::nats::{ConnectOptions, Auth, NatsOutputConfig};
    use crate::transport::nats::NatsOutputEndpoint;
    use feldera_adapterlib::transport::OutputEndpoint;
    use anyhow::Result as AnyResult;
    use async_nats;
    use futures_util::StreamExt;
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

    fn create_test_config(server_url: String) -> NatsOutputConfig {
        NatsOutputConfig {
            connection_config: ConnectOptions {
                server_url,
                auth: Auth::default(),
            },
            subject: "test.output".to_string(),
            headers: None,
            jetstream: None,
        }
    }

    #[test]
    fn test_endpoint_creation() {
        let config = create_test_config("nats://localhost:4222".to_string());
        let endpoint = NatsOutputEndpoint::new(config);
        assert!(endpoint.is_ok());
        
        let endpoint = endpoint.unwrap();
        assert!(!endpoint.is_fault_tolerant());
    }

    #[test]
    fn test_output_to_nats_server() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

            let config = create_test_config(nats_url);
            let mut endpoint = NatsOutputEndpoint::new(config)?;
            
            // Mock error callback
            let error_callback = |_fatal: bool, _error: anyhow::Error| {};
            endpoint.connect(Box::new(error_callback))?;

            // Subscribe to the subject to receive messages
            let mut subscriber = client.subscribe("test.output").await?;

            // Send test data
            let test_data = b"Hello, NATS!";
            endpoint.batch_start(0)?;
            endpoint.push_buffer(test_data)?;
            endpoint.batch_end()?;

            // Verify message was received
            tokio::time::timeout(Duration::from_secs(2), async {
                let message = subscriber.next().await.unwrap();
                assert_eq!(message.payload.as_ref(), test_data);
                assert_eq!(message.subject.as_str(), "test.output");
            })
            .await
            .map_err(|_| anyhow::anyhow!("Timeout waiting for message"))?;

            Ok::<(), anyhow::Error>(())
        })?;

        Ok(())
    }

    #[test]
    fn test_output_key_value_pairs() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

            let config = create_test_config(nats_url);
            let mut endpoint = NatsOutputEndpoint::new(config)?;
            
            let error_callback = |_fatal: bool, _error: anyhow::Error| {};
            endpoint.connect(Box::new(error_callback))?;

            let mut subscriber = client.subscribe("test.output").await?;

            // Send key-value pair
            endpoint.batch_start(0)?;
            endpoint.push_key(
                Some(b"key1"), 
                Some(b"value1"), 
                &[]
            )?;
            endpoint.batch_end()?;

            // Verify message was received with correct key:value format
            tokio::time::timeout(Duration::from_secs(2), async {
                let message = subscriber.next().await.unwrap();
                assert_eq!(message.payload.as_ref(), b"key1:value1");
            })
            .await
            .map_err(|_| anyhow::anyhow!("Timeout waiting for message"))?;

            Ok::<(), anyhow::Error>(())
        })?;

        Ok(())
    }

    #[tokio::test]
    async fn test_output_multiple_messages() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

        let config = create_test_config(nats_url);
        let mut endpoint = NatsOutputEndpoint::new(config)?;
        
        let error_callback = |_fatal: bool, _error: anyhow::Error| {};
        endpoint.connect(Box::new(error_callback))?;

        let mut subscriber = client.subscribe("test.output").await?;

        // Send multiple messages in a batch
        endpoint.batch_start(0)?;
        endpoint.push_buffer(b"message1")?;
        endpoint.push_buffer(b"message2")?;
        endpoint.push_key(Some(b"key"), Some(b"value"), &[])?;
        endpoint.batch_end()?;

        // Verify all messages were received
        let mut received_messages = Vec::new();
        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(2), async {
                let message = subscriber.next().await.unwrap();
                received_messages.push(message.payload.to_vec());
            })
            .await
            .map_err(|_| anyhow::anyhow!("Timeout waiting for message"))?;
        }

        assert_eq!(received_messages.len(), 3);
        assert!(received_messages.contains(&b"message1".to_vec()));
        assert!(received_messages.contains(&b"message2".to_vec()));
        assert!(received_messages.contains(&b"key:value".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_output_multiple_batches() -> AnyResult<()> {
        let (_nats_server, nats_url) = util::start_nats_and_get_address()?;
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;

        let config = create_test_config(nats_url);
        let mut endpoint = NatsOutputEndpoint::new(config)?;
        
        let error_callback = |_fatal: bool, _error: anyhow::Error| {};
        endpoint.connect(Box::new(error_callback))?;

        let mut subscriber = client.subscribe("test.output").await?;

        // Send multiple batches
        for i in 0..3 {
            endpoint.batch_start(i)?;
            endpoint.push_buffer(format!("batch_{}_message", i).as_bytes())?;
            endpoint.batch_end()?;
        }

        // Verify all messages were received
        let mut received_messages = Vec::new();
        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(2), async {
                let message = subscriber.next().await.unwrap();
                received_messages.push(String::from_utf8(message.payload.to_vec()).unwrap());
            })
            .await
            .map_err(|_| anyhow::anyhow!("Timeout waiting for message"))?;
        }

        assert_eq!(received_messages.len(), 3);
        assert!(received_messages.contains(&"batch_0_message".to_string()));
        assert!(received_messages.contains(&"batch_1_message".to_string()));
        assert!(received_messages.contains(&"batch_2_message".to_string()));

        Ok(())
    }
}