
CREATE TABLE ocamlmq_msgs(
    msg_id VARCHAR(255) NOT NULL PRIMARY KEY,
    ack_pending BOOL NOT NULL DEFAULT false,
    priority INT NOT NULL,
    destination VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ack_timeout FLOAT NOT NULL,
    body BYTEA NOT NULL
);

CREATE INDEX ocamlmq_msgs_ack_pending_destination_priority_timestamp ON ocamlmq_msgs
       USING BTREE(ack_pending, destination, priority, timestamp);
