
CREATE TABLE mq_server_msgs(
    id SERIAL NOT NULL PRIMARY KEY,
    msg_id VARCHAR(255) NOT NULL UNIQUE,
    priority INT NOT NULL,
    destination VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ack_timeout FLOAT NOT NULL,
    body TEXT NOT NULL
);

CREATE TABLE mq_server_ack_msgs(LIKE mq_server_msgs INCLUDING DEFAULTS);

CREATE INDEX mq_server_msgs_destination_priority_timestamp ON mq_server_msgs
       USING BTREE(priority, timestamp);

CREATE UNIQUE INDEX mq_server_ack_msgs_msg_id ON mq_server_ack_msgs USING BTREE(msg_id);
