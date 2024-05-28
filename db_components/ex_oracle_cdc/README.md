# Oracle SQL CDC Extractor

Description

**Table of contents:**

[TOC]

# Functionality

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
under the hood. The connector captures row-level changes in the schemas of an Oracle database. It uses the `LogMiner`
database package. This plug-in is always present so no additional libraries need to be
installed, but it must be properly configured in order for the connector to work.

The first time it connects to an Oracle server, the connector takes a consistent snapshot
of all schemas. After snapshot is complete, the connector continuously captures row-level changes that insert,
update, and delete database content and that were committed to an Oracle database.

**NOTE** The component abstracts the underlying Debezium connector configuration and provides a simplified interface for
the user. This means that only subset of the Debezium connector capabilities are exposed to the user.

### Performance considerations

Having multiple publications (connector configurations) can have performance implications. Each publication will have its own set of triggers and
other replication mechanisms, which can increase the load on the database server. However, this can also be beneficial
if different publications have different performance requirements or if they need to be replicated to different types of
subscribers with different capabilities.

## OracleDB Setup

### Set Recovery File Destination Size
```sql
ALTER SYSTEM SET db_recovery_file_dest_size = 10G;
```

### Set Recovery File Destination
```sql
ALTER SYSTEM SET db_recovery_file_dest = '/opt/oracle/oradata/ORCLCDB' scope=spfile;
```

### Restart the Database
```sql
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
```

### Enable Archivelog Mode
```sql
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ARCHIVE LOG LIST;
```

### Manage Redo Log Files
```sql
ALTER DATABASE CLEAR LOGFILE GROUP 1;
ALTER DATABASE DROP LOGFILE GROUP 1;
ALTER DATABASE ADD LOGFILE GROUP 1 ('/opt/oracle/oradata/ORCLCDB/redo01.log') SIZE 400M REUSE;
ALTER SYSTEM SWITCH LOGFILE;
```

### Add Supplemental Log Data
```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

### Create LogMiner Tablespace ???
```sql
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
```

### Create LogMiner Tablespace in PDB
```sql
CONNECT sys/oraclepw@ORCLPDB1 as sysdba;
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
```

### Create User and Grant Privileges
```sql
CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs CONTAINER=ALL;
GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL;
```

### Enabling the Heartbeat queries

You can enable heartbeat to force the connector to emit a heartbeat event at regular intervals so that offsets remain synchronized.

**Prerequisites**

Before enabling the Heartbeat signals, the Heartbeat table must be created in the source database. Recommended heartbeat table schema:

```sql
CREATE TABLE "C##DBZUSER".HEARTBEAT (
    id NUMBER PRIMARY KEY,
    last_heartbeat DATE
);
```

The connector will then perform UPDATE query on that table in the selected interval. It is recommended to use UPDATE query to avoid table bloat.

**Enable the heartbeat signals:**

- Set the `heartbeat > Heartbeat interval [ms]` connector configuration property to the desired interval in milliseconds. 
- Set the `heartbeat > Action query` connector configuration property to the desired query that will be executed on the heartbeat table.
  - It is recommended to use the default UPDATE query: `UPDATE kbc.heartbeat SET last_heartbeat = NOW()`
- Select the heartbeat table in the `Datasource > Tables to sync` configuration property to track the heartbeat table and make sure it is contained in the publication.


 ## Data Type Mapping

The MySQL datatypes are mapped to the [Keboola Connection Base Types](https://help.keboola.com/storage/tables/data-types/#base-types) as follows:

Based on the JSON file you've selected, the `base_type` column in the table can be updated as follows:

| source_type   | base_type     | note                                                                             |
|---------------|---------------|----------------------------------------------------------------------------------|
| tinyint       | INTEGER       |                                                                                  |
| smallint      | INTEGER       |                                                                                  |
| mediumint     | INTEGER       |                                                                                  |
| int           | INTEGER       |                                                                                  |
| integer       | INTEGER       |                                                                                  |
| bigint        | INTEGER       |                                                                                  |
| decimal       | NUMBER        |                                                                                  |
| dec           | NUMBER        |                                                                                  |
| numeric       | NUMBER        |                                                                                  |
| float         | FLOAT         |                                                                                  |
| double        | DOUBLE        |                                                                                  |
| real          | FLOAT         |                                                                                  |
| binary_float  | FLOAT         |                                                                                  |
| binary_double | DOUBLE        |                                                                                  |
| bit           | BOOLEAN       |                                                                                  |
| date          | DATE          | `YYYY-MM-DD` Format When Native Types are disabled                               |
| datetime      | TIMESTAMP     | `YYYY-MM-DD HH:MM:SS` Format When Native Types are disabled                      |
| timestamp     | TIMESTAMP_NTZ | `YYYY-MM-DD HH:MM:SS+TZ` Format When Native Types are disabled. TZ is always UTC |
| time          | TIME          |                                                                                  |
| year          | INTEGER       |                                                                                  |
| char          | STRING        |                                                                                  |
| nchar         | STRING        |                                                                                  |
| varchar       | STRING        |                                                                                  |
| nvarchar      | STRING        |                                                                                  |
| varchar2      | STRING        |                                                                                  |
| nvarchar2     | STRING        |                                                                                  |
| clob          | STRING        |                                                                                  |
| nclob         | STRING        |                                                                                  |
| binary        | STRING        |                                                                                  |
| varbinary     | STRING        |                                                                                  |
| blob          | STRING        |                                                                                  |
| long          | STRING        |                                                                                  |
| raw           | STRING        |                                                                                  |
| long raw      | STRING        |                                                                                  |
| bfile         | STRING        |                                                                                  |
| rowid         | STRING        |                                                                                  |
| urowid        | STRING        |                                                                                  |
| json          | STRING        |                                                                                  |
| xml           | STRING        |                                                                                  |


**Other types are not supported and such columns will be skipped from syncing.**

## System columns

Each result table will contain the following system columns:

| Name                    | Base Type | Note                                                                                                                                                                   |
|-------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| KBC__OPERATION          | STRING    | Event type, e.g. r - read on init sync; c - INSERT; u - UPDATE; d - DELETE                                                                                             |
| KBC__EVENT_TIMESTAMP_MS | TIMESTAMP | Source database transaction timestamp. MS since epoch if Native types are not enabled.                                                                                 |
| KBC__DELETED            | BOOLEAN   | True when the event is a delete event (the record is deleted)                                                                                                          |
| KBC__BATCH_EVENT_ORDER  | INTEGER   | Numerical order of the events in the current batch (extraction). You can use this in combination with KBC__EVENT_TIMESTAMP_MS to mark the latest event per record (ID) |

## Schema Drift

The connector is currently not capable of handling schema changes in the source database, e.g. `ADD`, `DROP` columns.

# Prerequisites

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
under the hood. The following instructions are partially taken from there.

This connector currently uses the native `LogMiner` plugin and supports Oracle versions 19, 21, and 23.
Currently, lower versions are not supported, but it is theoretically possible (please submit a feature request)

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


# Configuration

## Connection Settings

- **Host**: The hostname of the MySQL server.
- **Port**: The port number of the MySQL server.
- **User**: The username to be used to connect to the MySQL server.
- **Password**: The password to be used to connect to the MySQL server.
- **Database**: The name of the CDB to be used. (Debezium CDC needs both CDB and PDB to be specified.)
- **Pluggable Database (PDB)**: The name of the pluggable database to be used.

### SSH Tunnel

You may opt to use a SSH Tunnel to secure your connection. Find detailed instructions for setting up an SSH tunnel in
the [developer documentation](https://developers.keboola.com/integrate/database/. While setting up an SSH tunnel requires some work, it is the most reliable and secure
option for connecting to your database server.

## Data Source

- **Schemas**: The schemas to be included in the CDC.
- **Tables**: The tables to be included in the CDC. If left empty, all tables in the selected schemas will be included.

### Column Filters

The column filters are used to specify which columns should be included in the extraction. The lists can be defined as a
comma-separated list of
fully-qualified names, i.e. in the form `schemaName.tableName.columnName`.

To match the name of a column, connector applies the regular expression that you specify as an **anchored regular
expression**. That is, the expression is used to match the entire name string of the column; it does not match
substrings that might be present in a column name.

**TIP**: To test your regex expressions, you can use online tools such as [this one](https://regex101.com/).

- **Column Filter Type**: The column filter type to be used. The following options are available:
    - `None`: No filter applied, all columns in all tables will be extracted.
    - `Include List`: The columns to be included in the CDC.
    - `Exclude List`: The columns to be excluded from the CDC.
- **Column List**: List of the fully-qualified column names or regular expressions that match the columns to be included
  or excluded (based on the selected filter type).

## Sync Options

- **Signalling Table**: The name of the signalling table in the source database. The signalling table is used by the
  connector to store various signal events and incremental snapshot watermarks. See more in
  the [Signalling table](#signalling-table) section.
- **Replication Mode**: The replication mode to be used. The following options are available:
    - `Standard`: The connector performs an initial *consistent snapshot* of each of your databases. The connector reads
      the binlog from the point at which the snapshot was made.
    - `Changes only`: The connector reads the changes from the binlog immediately, skipping the initial load.
- **Snapshot Fetch Size**: During a snapshot, the connector reads table content in batches of rows. This property
  specifies the maximum number of rows in a batch. The default value is `10240`.
- **Snapshot parallelism**: Specifies the number of threads that the connector uses when performing an initial snapshot.
  To enable parallel initial snapshots, set the property to a value greater than 1. In a parallel initial snapshot, the
  connector processes multiple tables concurrently. Note that setting up high values may lead to OOM errors. Change the
  default value on your own risk.

### Heartbeat

Enable heartbeat signals to prevent WAL disk space consumption. The connector will periodically emit a heartbeat signal to the selected table.

**NOTE** The heartbeat signal must also be selected in the `Datasource > Tables to sync` configuration property. For more information, see the [WAL disk space consumption](#wal-disk-space-consumption) section.

- **Heartbeat interval [ms]**: The interval in milliseconds at which the heartbeat signal is emitted. The default value
  is `3000` (3 s).
- **Action Query**: The query that the connector uses to send heartbeat signals to the source database. The query must be
  a valid SQL statement that the source database can execute and the heartbeat table must be tracked in the Source
  settings.

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
cd python-cdc-component/db_components/ex-oracle-cdc
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
