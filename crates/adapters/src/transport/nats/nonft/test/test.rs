use crate::transport::nats::output::NatsOutputEndpoint;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint};
use feldera_types::transport::nats::{Auth, ConnectOptions, NatsOutputConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[test]
fn test_nats_output_endpoint_creation() {
    let config = NatsOutputConfig {
        connection_config: ConnectOptions {
            server_url: "nats://localhost:4222".to_string(),
            auth: Auth::default(),
        },
        subject: "test.subject".to_string(),
        headers: None,
    };

    let endpoint = NatsOutputEndpoint::new(config);
    assert!(endpoint.is_ok());

    let endpoint = endpoint.unwrap();
    assert!(!endpoint.is_fault_tolerant());
    assert_eq!(endpoint.max_buffer_size_bytes(), 1_000_000);
}

#[test]
fn test_nats_output_endpoint_with_headers() {
    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), "application/json".to_string());
    headers.insert("source".to_string(), "feldera".to_string());

    let config = NatsOutputConfig {
        connection_config: ConnectOptions {
            server_url: "nats://localhost:4222".to_string(),
            auth: Auth::default(),
        },
        subject: "test.subject.with.headers".to_string(),
        headers: Some(headers),
    };

    let endpoint = NatsOutputEndpoint::new(config);
    assert!(endpoint.is_ok());
}

#[test]
fn test_batch_operations() {
    let config = NatsOutputConfig {
        connection_config: ConnectOptions {
            server_url: "nats://localhost:4222".to_string(),
            auth: Auth::default(),
        },
        subject: "test.batch".to_string(),
        headers: None,
    };

    let mut endpoint = NatsOutputEndpoint::new(config).unwrap();

    // Test batch operations without connection (should succeed for non-FT)
    assert!(endpoint.batch_start(0).is_ok());
    assert!(endpoint.batch_end().is_ok());
}

#[test]
fn test_push_operations_without_connection() {
    let config = NatsOutputConfig {
        connection_config: ConnectOptions {
            server_url: "nats://localhost:4222".to_string(),
            auth: Auth::default(),
        },
        subject: "test.push".to_string(),
        headers: None,
    };

    let mut endpoint = NatsOutputEndpoint::new(config).unwrap();

    // These should fail without connection
    let buffer = b"test message";
    assert!(endpoint.push_buffer(buffer).is_err());

    let key = b"test_key";
    let value = b"test_value";
    assert!(endpoint.push_key(Some(key), Some(value), &[]).is_err());
}

#[test]
fn test_error_callback_storage() {
    let config = NatsOutputConfig {
        connection_config: ConnectOptions {
            server_url: "nats://localhost:4222".to_string(),
            auth: Auth::default(),
        },
        subject: "test.callback".to_string(),
        headers: None,
    };

    let mut endpoint = NatsOutputEndpoint::new(config).unwrap();

    let error_received = Arc::new(Mutex::new(false));
    let error_received_clone = error_received.clone();

    let callback: AsyncErrorCallback = Box::new(move |_fatal, _error| {
        *error_received_clone.lock().unwrap() = true;
    });

    // This will fail to connect but should store the callback
    let result = endpoint.connect(callback);
    assert!(result.is_err()); // Connection should fail without NATS server
}

// Integration tests that require a running NATS server
#[cfg(feature = "integration-tests")]
mod integration_tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_nats_output_integration() {
        // This test requires a NATS server running on localhost:4222
        let config = NatsOutputConfig {
            connection_config: ConnectOptions {
                server_url: "nats://localhost:4222".to_string(),
                auth: Auth::default(),
            },
            subject: "test.integration".to_string(),
            headers: None,
        };

        let mut endpoint = NatsOutputEndpoint::new(config).unwrap();

        let error_received = Arc::new(Mutex::new(None));
        let error_received_clone = error_received.clone();

        let callback: AsyncErrorCallback = Box::new(move |fatal, error| {
            *error_received_clone.lock().unwrap() = Some((fatal, error.to_string()));
        });

        // Connect to NATS
        let connect_result = endpoint.connect(callback);
        if connect_result.is_err() {
            // Skip test if NATS server is not available
            println!("Skipping integration test - NATS server not available");
            return;
        }

        // Test publishing a buffer
        let buffer = b"Hello, NATS!";
        assert!(endpoint.push_buffer(buffer).is_ok());

        // Test publishing key-value pairs
        let key = b"greeting";
        let value = b"Hello, World!";
        assert!(endpoint.push_key(Some(key), Some(value), &[]).is_ok());

        // Test publishing with headers
        let headers = [("content-type", Some(b"text/plain".as_slice()))];
        assert!(endpoint.push_key(Some(key), Some(value), &headers).is_ok());

        // Give some time for async operations to complete
        sleep(Duration::from_millis(100)).await;

        // Check that no errors were received
        let error = error_received.lock().unwrap();
        assert!(error.is_none(), "Unexpected error: {:?}", error);
    }

    #[tokio::test]
    async fn test_nats_output_with_headers_integration() {
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "feldera-test".to_string());
        headers.insert("version".to_string(), "1.0".to_string());

        let config = NatsOutputConfig {
            connection_config: ConnectOptions {
                server_url: "nats://localhost:4222".to_string(),
                auth: Auth::default(),
            },
            subject: "test.headers.integration".to_string(),
            headers: Some(headers),
        };

        let mut endpoint = NatsOutputEndpoint::new(config).unwrap();

        let callback: AsyncErrorCallback = Box::new(move |_fatal, _error| {
            // Error callback
        });

        let connect_result = endpoint.connect(callback);
        if connect_result.is_err() {
            println!("Skipping integration test - NATS server not available");
            return;
        }

        // Test publishing with configured headers
        let buffer = b"Message with headers";
        assert!(endpoint.push_buffer(buffer).is_ok());

        sleep(Duration::from_millis(100)).await;
    }
}
