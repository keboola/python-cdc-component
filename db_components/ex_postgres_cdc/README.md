# Postgre SQL CDC Extractor

Description

**Table of contents:**

[TOC]

# Functionality

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
under the hood.

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

### Performance considerations

Having multiple publications (connector configurations) can have performance implications. Each publication will have its own set of triggers and
other replication mechanisms, which can increase the load on the database server. However, this can also be beneficial
if different publications have different performance requirements or if they need to be replicated to different types of
subscribers with different capabilities.

# Prerequisites

This connector uses [Debezium connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
under the hood. The following instructions are partially taken from there.

This connector currently uses the native `pgoutput` logical replication stream support that is available only
in `PostgreSQL 10+`.
Currently, lower versions are not supported, but it is theoretically possible (please submit a feature request)

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

## Destination

The destination is a mandatory configuration option. It is used to specify how the data is loaded into the destination
tables.

### Load Type

The `Load Type` configuration option specifies how the data is loaded into the destination tables.

The following options are available:

- `Incremental Load - Deduplicated`: The connector upserts records into the destination table. The connector uses the
  primary key to perform upsert. The connector does not delete records from the destination table.
- `Incremental Load - Append`: The connector produces no primary key. The order of the events will be given by
  the `KBC__EVENT_TIMESTAMP_MS` column + helper `KBC__EVENT_ORDER` column which contains the order in one batch.
- `Full Load - Deduplicated`: The destination table data will be replaced with the current batch and deduplicated by the
  primary key.
- `Full Load - Append`: The destination table data will be replaced with the current batch and the batch won't be
  deduplicated.

**NOTE:** In case of `Append` modes additional columns `KBC__EVENT_TIMESTAMP_MS` and `KBC__EVENT_ORDER` are added to the destination table.

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
git clone git@bitbucket.org:kds_consulting_team/kds-team.ex-postgres-cdc.git kds-team.ex-postgres-cdc
cd kds-team.ex-postgres-cdc
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
