# Postgre SQL CDC Extractor

Description

**Table of contents:**

[TOC]

# Functionality

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
under the hood.


 ## Data Type Mapping

The MySQL datatypes are mapped to the [Keboola Connection Base Types](https://help.keboola.com/storage/tables/data-types/#base-types) as follows:

| Source Type | Base Type          | Note                                                                             |
|-------------|--------------------|----------------------------------------------------------------------------------|
| INTEGER     | INTEGER            |                                                                                  |
| TINYINT     | INTEGER            |                                                                                  |
| SMALLINT    | INTEGER            |                                                                                  |
| MEDIUMINT   | INTEGER            |                                                                                  |
| BIGINT      | INTEGER            |                                                                                  |
| FLOAT       | NUMERIC            |                                                                                  |
| DOUBLE      | NUMERIC            |                                                                                  |
| DECIMAL     | NUMERIC            |                                                                                  |
| DATE        | DATE               | `YYYY-MM-DD` Format When Native Types are disabled                               |
| DATETIME    | TIMESTAMP          | `YYYY-MM-DD HH:MM:SS` Format When Native Types are disabled                      |
| TIMESTAMP   | TIMESTAMP          | `YYYY-MM-DD HH:MM:SS+TZ` Format When Native Types are disabled. TZ is always UTC |
| TIME        | TIMESTAMP          |                                                                                  |
| YEAR        | INTEGER(4)         |                                                                                  |
| CHAR        | STRING             |                                                                                  |
| VARCHAR     | STRING             |                                                                                  |
| BLOB        | STRING(65535)      | Representation depends on the selected Binary Handling Mode                      |
| TEXT        | STRING(65535)      |                                                                                  |
| TINYBLOB    | STRING             | Representation depends on the selected Binary Handling Mode                      |
| TINYTEXT    | STRING             |                                                                                  |
| MEDIUMBLOB  | STRING             | Representation depends on the selected Binary Handling Mode                      |
| MEDIUMTEXT  | STRING             |                                                                                  |
| LONGBLOB    | STRING(2147483647) | Representation depends on the selected Binary Handling Mode                      |
| LONGTEXT    | STRING(2147483647) |                                                                                  |
| ENUM        | STRING             |                                                                                  |
| SET         | STRING             |                                                                                  |
| BIT         | STRING             |                                                                                  |
| BINARY      | STRING             |                                                                                  |
| VARBINARY   | STRING             |                                                                                  |
| GEOMETRY    | STRING(65535)      |                                                                                  |
| JSON        | STRING(1073741824) |                                                                                  |
| BOOLEAN     | BOOLEAN            |                                                                                  |
| BIT(1)      | BOOLEAN            |                                                                                  |

## System columns

Each result table will contain the following system columns:

| Name                    | Base Type | Note                                                                                                                                                                   |
|-------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| KBC__OPERATION          | STRING    | Event type, e.g. r - read on init sync; c - INSERT; u - UPDATE; d - DELETE                                                                                             |
| KBC__EVENT_TIMESTAMP_MS | TIMESTAMP | Source database transaction timestamp. MS since epoch if Native types are not enabled.                                                                                 |
| KBC__DELETED            | BOOLEAN   | True when the event is a delete event (the record is deleted)                                                                                                          |
| KBC__BATCH_EVENT_ORDER  | INTEGER   | Numerical order of the events in the current batch (extraction). You can use this in combination with KBC__EVENT_TIMESTAMP_MS to mark the latest event per record (ID) |


## Schema Drift


# Prerequisites

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
under the hood. The following instructions are partially taken from there.


## Signalling table

The connector needs access to a signalling table in the source database. The signalling table is used by to connector to store various signal events and incremental snapshot watermarks.

### Creating a signaling data collection

You create a signaling table by submitting a standard SQL DDL query to the source database.

**Prerequisites**

You have sufficient access privileges to create a table on the source database.

**Procedure**

Submit a SQL query to the source database to create a table that is consistent with the required structure, as shown in the following example:

The following example shows a CREATE TABLE command that creates a three-column debezium_signal table:

`CREATE TABLE debezium_signal (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data TEXT NULL);`

### MySQL Setup



# Configuration

## Destination

The destination is a mandatory configuration option. It is used to specify how the data is loaded into the destination
tables.

### Load Type

The `Load Type` configuration option specifies how the data is loaded into the destination tables.

The following options are available:

- `Incremental Load - Deduplicated`: The connector upserts records into the destination table. The connector uses the
  primary key to perform upsert. The connector does not delete records from the destination table.
- `Incremental Load - Append`: The connector produces no primary key. The order of the events will be given by
  the `KBC__EVENT_TIMESTAMP_MS` column + helper `KBC__BATCH_EVENT_ORDER` column which contains the order in one batch.
- `Full Load - Deduplicated`: The destination table data will be replaced with the current batch and deduplicated by the
  primary key.
- `Full Load - Append`: The destination table data will be replaced with the current batch and the batch won't be
  deduplicated.

**NOTE:** In case of `Append` modes additional columns `KBC__EVENT_TIMESTAMP_MS` and `KBC__BATCH_EVENT_ORDER` are added to the destination table.

# Development

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/python-cdc-component.git
cd python-cdc-component/db_components/ex-postgresql-cdc
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
