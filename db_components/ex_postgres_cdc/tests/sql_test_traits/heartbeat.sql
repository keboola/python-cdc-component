CREATE SCHEMA IF NOT EXISTS kbc;
DROP TABLE IF EXISTS kbc.heartbeat;
CREATE TABLE kbc.heartbeat (id SERIAL PRIMARY KEY, last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW());
INSERT INTO kbc.heartbeat (last_heartbeat) VALUES (NOW());