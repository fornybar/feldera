CREATE TABLE my_numbers (
    number INTEGER
) WITH (
    'connectors' = '[{
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://localhost:4222"
                },
                "stream_name": "numbers_stream",
                "consumer_config": {
                    "deliver_policy": "All",
                    "filter_subjects": ["numbers.topic"]
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
