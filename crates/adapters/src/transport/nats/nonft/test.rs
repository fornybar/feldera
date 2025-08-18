#[cfg(test)]
mod tests {
    use feldera_types::transport::nats::{ConnectOptions, Auth, NatsOutputConfig};
    use crate::transport::nats::NatsOutputEndpoint;
    use feldera_adapterlib::transport::OutputEndpoint;

    fn create_test_config() -> NatsOutputConfig {
        NatsOutputConfig {
            connection_config: ConnectOptions {
                server_url: "nats://localhost:4222".to_string(),
                auth: Auth::default(),
            },
            subject: "test.subject".to_string(),
            headers: None,
            jetstream: None,
        }
    }

    #[test]
    fn test_endpoint_creation() {
        let config = create_test_config();
        let endpoint = NatsOutputEndpoint::new(config);
        assert!(endpoint.is_ok());
        
        let endpoint = endpoint.unwrap();
        assert!(!endpoint.is_fault_tolerant());
    }
}