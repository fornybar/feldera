//! Transport adapter for NATS

mod input;
pub mod ft;
pub mod nonft;

pub use input::NatsInputEndpoint;
pub use ft::NatsFtOutputEndpoint;
pub use nonft::NatsOutputEndpoint;
