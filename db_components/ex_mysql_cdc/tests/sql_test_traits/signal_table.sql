DROP TABLE IF EXISTS inventory.debezium_signals;

CREATE TABLE IF NOT EXISTS inventory.debezium_signals
(
    id    VARCHAR(255) NOT NULL PRIMARY KEY,
    type  VARCHAR(32) NOT NULL,
    data  VARCHAR(2048)
);