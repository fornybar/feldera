For testing have a postgres database running:
docker run -it -e "POSTGRES_PASSWORD=password" -p 5432:5432 postgres

and then start pipline manager with:
cargo run -p pipeline-manager -- --db-connection-string postgresql://postgres:password@localhost:5432/postgres


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
                "update_format": "insert_delete"
            }
        }
    }]'
);

for insert_detele use
nats -s localhost:4222 pub -J numbers.topic '{"insert": {"number": 12}}'


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

for raw use:
nats -s localhost:4222 pub -J numbers.topic '{"number": 12}'
