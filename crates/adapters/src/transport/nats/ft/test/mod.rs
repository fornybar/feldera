#[cfg(test)]
mod tests {
    use feldera_types::transport::nats::{ConnectOptions, Auth, JetStreamConfig, NatsOutputConfig};
    use crate::transport::nats::NatsFtOutputEndpoint;
    use super::super::output::OutputPosition;
    use feldera_adapterlib::transport::OutputEndpoint;

    fn create_test_config() -> NatsOutputConfig {
        NatsOutputConfig {
            connection_config: ConnectOptions {
                server_url: "nats://localhost:4222".to_string(),
                auth: Auth::default(),
            },
            subject: "test.subject".to_string(),
            headers: None,
            jetstream: Some(JetStreamConfig {
                stream_name: "test_stream".to_string(),
                enable_fault_tolerance: true,
                max_age: None,
                max_bytes: None,
                max_messages: None,
            }),
        }
    }

    #[test]
    fn test_fault_tolerant_endpoint_creation() {
        let config = create_test_config();
        let endpoint = NatsFtOutputEndpoint::new(config);
        assert!(endpoint.is_ok());
        
        let endpoint = endpoint.unwrap();
        assert!(endpoint.is_fault_tolerant());
    }

    #[test]
    fn test_fault_tolerant_endpoint_requires_jetstream() {
        let mut config = create_test_config();
        config.jetstream = None;
        
        let endpoint = NatsFtOutputEndpoint::new(config);
        assert!(endpoint.is_err());
        assert!(endpoint.unwrap_err().to_string().contains("JetStream configuration required"));
    }

    #[test]
    fn test_fault_tolerant_endpoint_requires_ft_enabled() {
        let mut config = create_test_config();
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
}