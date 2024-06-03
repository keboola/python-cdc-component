The connector captures row-level changes in the schemas of a PostgreSQL database. It uses the `pgoutput`
logical decoding output plug-in available in PostgreSQL 10+. It is maintained by the PostgreSQL community, and
used by PostgreSQL itself for logical replication. This plug-in is always present so no additional libraries need to be
installed.

The first time it connects to a PostgreSQL server or cluster, the connector takes a consistent snapshot
of all schemas. After that snapshot is complete, the connector continuously captures row-level changes that insert,
update, and delete database content and that were committed to a PostgreSQL database.


**Functionality** 

- The connector performs an initial sync of the tables.
  - This step can be skipped or modified by custom snapshot queries for each table. 
- The connector reads the WAL log, produces change events for row-level `INSERT`, `UPDATE`, and `DELETE` operations.
- The schema changes are handled automatically without interruption and without needing a re-sync.
- You can choose to keep all changes in the resulting tables or deduplicate the batch result so that only the latest states of each record are reflected in the result table.
- The connector uses micro-batches, so you can schedule them as often as needed, whether every 5 minutes or twice a day.
  - This approach gives you more control over the resulting cost of the solution and lets you leverage the power of log-based replication even for smaller use cases if needed.
  - If your use case requires short interval syncs, reach out to us for information about a cost-efficient CDC add-on license.