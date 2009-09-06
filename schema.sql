
CREATE TABLE mq_server_msgs(
    id SERIAL NOT NULL PRIMARY KEY,
    destination VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    body TEXT NOT NULL
);

CREATE INDEX mq_server_msgs_destination ON mq_server_msgs
       USING BTREE(body);
