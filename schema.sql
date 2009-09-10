
CREATE TABLE ocamlmq_msgs(
    id SERIAL NOT NULL PRIMARY KEY,
    msg_id VARCHAR(255) NOT NULL UNIQUE,
    priority INT NOT NULL,
    destination VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ack_timeout FLOAT NOT NULL,
    body BYTEA NOT NULL
);

CREATE TABLE ocamlmq_pending_acks(
    msg_id VARCHAR(255) NOT NULL PRIMARY KEY
    REFERENCES ocamlmq_msgs(msg_id) ON DELETE CASCADE
);

CREATE INDEX ocamlmq_msgs_destination_priority_timestamp ON ocamlmq_msgs
       USING BTREE(destination, priority, timestamp);
