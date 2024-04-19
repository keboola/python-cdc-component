-- This is example SQL code in PostgreSQL format. Convert this code to the format of your database.
DROP TABLE IF EXISTS inventory.debezium_signals;
CREATE TABLE IF NOT EXISTS inventory.debezium_signals ("id" TEXT NOT NULL PRIMARY KEY, "type" varchar(32) NOT NULL,"data" varchar(2048) NULL);