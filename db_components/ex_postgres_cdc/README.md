# Postgre SQL CDC Extractor

Description

**Table of contents:**

[TOC]

# Functionality

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
under the hood. The connector captures row-level changes in the schemas of a PostgreSQL database. It uses the `pgoutput`
logical decoding output plug-in available in PostgreSQL 10+. It is maintained by the PostgreSQL community, and
used by PostgreSQL itself for logical replication. This plug-in is always present so no additional libraries need to be
installed.

The first time it connects to a PostgreSQL server or cluster, the connector takes a consistent snapshot
of all schemas. After that snapshot is complete, the connector continuously captures row-level changes that insert,
update, and delete database content and that were committed to a PostgreSQL database.

**NOTE** The component abstracts the underlying Debezium connector configuration and provides a simplified interface for
the user. This means that only subset of the Debezium connector capabilities are exposed to the user.

## Publication creation

The connector requires user with `rds_replication` role.  
To enable a user account other than the master account to initiate logical replication,
you must grant the account the rds_replication role. For example, `grant rds_replication to <my_user>`.

**The connector handles the publication creation automatically**

- If a publication exists, the connector uses it.
- If no publication exists, the connector creates a new publication for tables that match the currently selected schemas
  and tables in the `Datasource` connector configuration property. For
  example: `CREATE PUBLICATION <publication_name> FOR TABLE <tbl1, tbl2, tbl3>`.
- If the publication exists, the connector updates the publication for
  tables that match the current configuration. For
  example: `ALTER PUBLICATION <publication_name> SET TABLE <tbl1, tbl2, tbl3>.`

### Publication Names

Note that each configuration of the connector creates a new publication with a unique name. The publication name contains 
configuration_id and alternatively branch id if it's a branch configuration. The publication name is generated as follows:
- "kbc_publication_{config_id}_prod" for production configuration
- "kbc_publication_{config_id}_dev_{branch_id}" for branch configuration.

**NOTE** be careful when running configurations in a Development branch. Once the branch is deleted, the assigned publication still exists 
and it's not deleted automatically. It is recommended to clean up any unused dev publications manually or using a script.

### Slot Names

Note that each configuration of the connector creates a new slot with a unique name. The slot name contains 
configuration_id and alternatively branch id if it's a branch configuration. The slot name is generated as follows:
- "slot_kbc_publication_{config_id}_prod" for production configuration
- "kbc_publication_{config_id}_dev_{branch_id}" for branch configuration.

### Performance considerations

Having multiple publications (connector configurations) can have performance implications. Each publication will have its own set of triggers and
other replication mechanisms, which can increase the load on the database server. However, this can also be beneficial
if different publications have different performance requirements or if they need to be replicated to different types of
subscribers with different capabilities.


## WAL disk space consumption
In certain cases, it is possible for PostgreSQL disk space consumed by WAL files to spike or increase out of usual proportions. There are several possible reasons for this situation:

- The LSN up to which the connector has received data is available in the `confirmed_flush_lsn` column of the server’s `pg_replication_slots` view. Data that is older than this LSN is no longer available, and the database is responsible for reclaiming the disk space.
  
  - Also in the `pg_replication_slots` view, the `restart_lsn` column contains the LSN of the oldest WAL that the connector might require. If the value for confirmed_flush_lsn is regularly increasing and the value of restart_lsn lags then the database needs to reclaim the space. 
  - The database typically reclaims disk space in batch blocks. This is expected behavior and no action by a user is necessary.

- There are many updates in a database that is being tracked but only a tiny number of updates are related to the table(s) and schema(s) for which the connector is capturing changes. This situation can be easily solved with periodic heartbeat events. Set the heartbeat.interval.ms connector configuration property.

**NOTE:** For the connector to detect and process events from a heartbeat table, you must add the table to the PostgreSQL publication created by the connector. **You can do that by selecting the heartbeat table in the `Datasource > Tables to sync` configuration property.**

- The PostgreSQL instance contains multiple databases and one of them is a high-traffic database. Debezium captures changes in another database that is low-traffic in comparison to the other database. Debezium then cannot confirm the LSN as replication slots work per-database and Debezium is not invoked. As WAL is shared by all databases, the amount used tends to grow until an event is emitted by the database for which Debezium is capturing changes. To overcome this, it is necessary to:
  - Enable periodic heartbeat record generation with the `heartbeat > interval.ms` connector configuration property.
  - Regularly emit change events from the database for which Debezium is capturing changes.

A separate process would then periodically update the table by either inserting a new row or repeatedly updating the same row. PostgreSQL then invokes Debezium, which confirms the latest LSN and allows the database to reclaim the WAL space. This task can be automated by means of the`heart beat > action query` connector configuration property.

**TIP:** For users on AWS RDS with PostgreSQL, a situation similar to the high traffic/low traffic scenario can occur in an idle environment. AWS RDS causes writes to its own system tables to be invisible to clients on a frequent basis (5 minutes). Again, regularly emitting events solves the problem.

### Enabling the Heartbeat queries

**Prerequisites**

Before enabling the Heartbeat signals, the Heartbeat table must be created in the source database. Recommended heartbeat table schema:

```sql
CREATE SCHEMA IF NOT EXISTS kbc;
CREATE TABLE kbc.heartbeat (id SERIAL PRIMARY KEY, last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW());
INSERT INTO kbc.heartbeat (last_heartbeat) VALUES (NOW());
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

| source_type              | base_type | note                                                                                          |
|--------------------------|-----------|-----------------------------------------------------------------------------------------------|
| INTEGER                  | INTEGER   |                                                                                               |
| SMALLINT                 | INTEGER   |                                                                                               |
| INTEGER                  | INTEGER   |                                                                                               |
| INTEGER                  | INTEGER   |                                                                                               |
| BIGINT                   | INTEGER   |                                                                                               |
| DECIMAL                  | NUMERIC   |                                                                                               |
| NUMERIC                  | NUMERIC   |                                                                                               |
| REAL                     | NUMERIC   |                                                                                               |
| DOUBLE PRECISION         | STRING    |                                                                                               |
| SMALLSERIAL              | INTEGER   |                                                                                               |
| SERIAL                   | INTEGER   |                                                                                               |
| BIGSERIAL                | INTEGER   |                                                                                               |
| MONEY                    | NUMERIC   |                                                                                               |
| CHARACTER                | STRING    |                                                                                               |
| CHAR                     | STRING    |                                                                                               |
| CHARACTER VARYING        | STRING    |                                                                                               |
| VARCHAR                  | STRING    |                                                                                               |
| TEXT                     | STRING    |                                                                                               |
| BYTEA                    | STRING    |                                                                                               |
| TIMESTAMP                | TIMESTAMP |                                                                                               |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP |                                                                                               |
| DATE                     | DATE      |                                                                                               |
| TIME                     | TIMESTAMP | A string representation of a timestamp with timezone information, where the timezone is GMT.  |
| TIME WITH TIME ZONE      | TIMESTAMP | A string representation of a time value with timezone information, where the timezone is GMT. |
| INTERVAL                 | STRING    |                                                                                               |
| BOOLEAN                  | BOOLEAN   |                                                                                               |
| POINT                    | STRING    |                                                                                               |
| CIDR                     | STRING    |                                                                                               |
| INET                     | STRING    |                                                                                               |
| MACADDR                  | STRING    |                                                                                               |
| MACADDR8                 | STRING    |                                                                                               |
| BIT                      | STRING    |                                                                                               |
| BIT VARYING              | STRING    |                                                                                               |
| UUID                     | STRING    |                                                                                               |
| XML                      | STRING    |                                                                                               |
| JSON                     | STRING    |                                                                                               |
| JSONB                    | STRING    |                                                                                               |
| INTEGER[]                | STRING    |                                                                                               |
| INT4RANGE                | STRING    |                                                                                               |
| LTREE                    | STRING    | Contains the string representation of a PostgreSQL LTREE value.                               |
| CITEXT                   | STRING    |                                                                                               |

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

The connector is capable of seamlessly handling schema changes in the source database, e.g. `ADD`, `DROP` columns.

The schema changes are handled in a following manner:

- **ADD column**
    - Such column is added to the destination table. Historic values will be empty (default not reflected).
- **DROP column**
    - The column will remain in the destination table.
    - The column NOT NULL constraint will be overridden and removed if present.
    - It's values will be NULL/EMPTY since the deletion.

# Prerequisites

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
under the hood. The following instructions are partially taken from there.

This connector currently uses the native `pgoutput` logical replication stream support that is available only
in `PostgreSQL 10+`.
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

### PostgreSQL Setup

For this connector to work it is necessary to enable a replication slot, and configure a user with sufficient privileges
to perform the replication.

### PostgreSQL in the Cloud

#### PostgreSQL on Amazon RDS

It is possible to capture changes in a PostgreSQL database that is running in
link:[Amazon RDS](https://aws.amazon.com/rds/). To do this:

* Set the instance parameter `rds.logical_replication` to `1`.
* Verify that the `wal_level` parameter is set to `logical` by running the query `SHOW wal_level` as the database RDS
  master user.
  This might not be the case in multi-zone replication setups.
  You cannot set this option manually.
  It is
  link: [automatically changed](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.html)
  when the `rds.logical_replication` parameter is set to `1`.
  If the `wal_level` is not set to `logical` after you make the preceding change, it is probably because the instance
  has to be restarted after the parameter group change.
  Restarts occur during your maintenance window, or you can initiate a restart manually.
* Set the {prodname} `plugin.name` parameter to `pgoutput`.
* Initiate logical replication from an AWS account that has the `rds_replication` role.
  The role grants permissions to manage logical slots and to stream data using logical slots.
  By default, only the master user account on AWS has the `rds_replication` role on Amazon RDS.
  To enable a user account other than the master account to initiate logical replication, you must grant the account
  the `rds_replication` role.
  For example, `grant rds_replication to _<my_user>_`. You must have `superuser` access to grant the `rds_replication`
  role to a user.
  To enable accounts other than the master account to create an initial snapshot, you must grant `SELECT` permission to
  the accounts on the tables to be captured.
  For more information about security for PostgreSQL logical replication, see the
  link:[PostgreSQL documentation](https://www.postgresql.org/docs/current/logical-replication-security.html).

#### PostgreSQL on Azure

It is possible to use {prodname} with: [Azure Database for PostgreSQL](https://docs.microsoft.com/azure/postgresql/),
which has support for the `pgoutput` logical decoding.

Set the Azure replication support to `logical`. You can use the
link:[Azure CLI](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#using-azure-cli) or
the: [Azure Portal](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#using-azure-portal) to configure
this. For example, to use the Azure CLI, here are
the: [`az postgres server`](https://docs.microsoft.com/cli/azure/postgres/server?view#azure-cli-latest) commands that
you need to execute:

```
az postgres server configuration set --resource-group mygroup --server-name myserver --name azure.replication_support --value logical

az postgres server restart --resource-group mygroup --name myserver
```

#### PostgreSQL on CrunchyBridge

It is possible to use {prodname} with: [CrunchyBridge](https://crunchybridge.com/); logical replication is already
turned on. The `pgoutput` plugin is available. You will have to create a replication user and provide correct
privileges.

### Configuring the PostgreSQL server

. To configure the replication slot regardless of the decoder being used, specify the following in the `postgresql.conf`
file:

```
# REPLICATION
wal_level = logical             // Instructs the server to use logical decoding with the write-ahead log.
```

Depending on your requirements, you may have to set other PostgreSQL streaming replication parameters when using
{prodname}.
Examples include `max_wal_senders` and `max_replication_slots` for increasing the number of connectors that can access
the sending server concurrently, and `wal_keep_size` for limiting the maximum WAL size which a replication slot will
retain.
For more information about configuring streaming replication, see the
link:https://www.postgresql.org/docs/current/runtime-config-replication.html#RUNTIME-CONFIG-REPLICATION-SENDER[PostgreSQL
documentation].

Debezium uses PostgreSQL's logical decoding, which uses replication slots.
Replication slots are guaranteed to retain all WAL segments required for Debezium even during Debezium outages. For this
reason, it is important to closely monitor replication slots to avoid too much disk consumption and other conditions
that can happen such as catalog bloat if a replication slot stays unused for too long.
For more information, see
the [PostgreSQL streaming replication documentation](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS).

If you are working with a `synchronous_commit` setting other than `on`,
the recommendation is to set `wal_writer_delay` to a value such as 10 milliseconds to achieve a low latency of change
events.
Otherwise, its default value is applied, which adds a latency of about 200 milliseconds.

**TIP:** Reading and
understanding [PostgreSQL documentation about the mechanics and configuration of the PostgreSQL write-ahead log](https://www.postgresql.org/docs/current/static/wal-configuration.html)
is strongly recommended.
endif::community[]

## Setting up permissions

The connector requires appropriate permissions to:

- SELECT tables (to perform initial syncs)
- `rds_replication`
-

The connector requires user with `rds_replication` role.  
To enable a user account other than the master account to initiate logical replication,
you must grant the account the rds_replication role. For example, `grant rds_replication to <my_user>`.

Setting up a PostgreSQL server to run a Debezium connector requires a database user that can perform replications.
Replication can be performed only by a database user that has appropriate permissions and only for a configured number
of hosts.

Although, by default, superusers have the necessary `REPLICATION` and `LOGIN` roles, it is best **not to provide the
Keboola replication user with elevated privileges**.
Instead, create a Keboola user that has the minimum required privileges.

**PostgreSQL administrative permissions.**

To provide a user with replication permissions, define a PostgreSQL role that has _at least_ the `REPLICATION`
and `LOGIN` permissions, and then grant that role to the user.
For example:

```CREATE ROLE __<name>__ REPLICATION LOGIN;```

**Setting privileges to enable Keboola to create PostgreSQL publications when you use `pgoutput`:**

Keboola(Debezium) streams change events for PostgreSQL source tables from _publications_ that are created for the
tables.
Publications contain a filtered set of change events that are generated from one or more tables.
The data in each publication is filtered based on the publication specification.
The specification can be created by the PostgreSQL database administrator or by the {prodname} connector.
To permit the {prodname} PostgreSQL connector to create publications and specify the data to replicate to them, the
connector must operate with specific privileges in the database.

There are several options for determining how publications are created.
In general, it is best to manually create publications for the tables that you want to capture, before you set up the
connector.
However, you can configure your environment in a way that permits Keboola connector to create publications
automatically, and to specify the data that is added to them.

Keboola connector uses include list and exclude list properties to specify how data is inserted in the publication.

For Keboola connector to create a PostgreSQL publication, it must run as a user that has the following privileges:

* Replication privileges in the database to add the table to a publication.
* `CREATE` privileges on the database to add publications.
* `SELECT` privileges on the tables to copy the initial table data. Table owners automatically have `SELECT` permission
  for the table.

To add tables to a publication, the user must be an owner of the table.
But because the source table already exists, you need a mechanism to share ownership with the original owner.
To enable shared ownership, you create a PostgreSQL replication group, and then add the existing table owner and the
replication user to the group.

1. Create a replication group.

```CREATE ROLE _<replication_group>_;```

2. Add the original owner of the table to the group.

```
GRANT REPLICATION_GROUP TO __<original_owner>__;
```

3. Add the {prodname} replication user to the group.

```
GRANT REPLICATION_GROUP TO __<replication_user>__;
```

4. Transfer ownership of the table to `<replication_group>`.

```
ALTER TABLE __<table_name>__ OWNER TO REPLICATION_GROUP;
```

# Configuration

## Connection Settings

- **Host**: The hostname of the MySQL server.
- **Port**: The port number of the MySQL server.
- **User**: The username to be used to connect to the MySQL server.
- **Password**: The password to be used to connect to the MySQL server.

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
- **Binary data handler**: Specifies how binary columns, for example, blob, binary, varbinary, should be represented in
  change events. The following options are available:
    - `Base64`: represents binary data as a base64-encoded String.
    - `Base64-url-safe`: represents binary data as a base64-url-safe-encoded String.
    - `Hex`: represents binary data as a hex-encoded (base16) String.
    - `Bytes`: represents binary data as a byte array.
- **Snapshot Fetch Size**: During a snapshot, the connector reads table content in batches of rows. This property
  specifies the maximum number of rows in a batch. The default value is `10240`.
- **Snapshot parallelism**: Specifies the number of threads that the connector uses when performing an initial snapshot.
  To enable parallel initial snapshots, set the property to a value greater than 1. In a parallel initial snapshot, the
  connector processes multiple tables concurrently. Note that setting up high values may lead to OOM errors. Change the
  default value on your own risk.

### Hearbeat

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
