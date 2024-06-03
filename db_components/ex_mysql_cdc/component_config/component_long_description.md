MySQL has a binary log (binlog) that records all operations in the order in which they are committed to the database.
This includes changes to table schemas and changes to the table data. MySQL uses the binlog for replication and
recovery. This MySQL connector reads the binlog, produces change events for row-level `INSERT`, `UPDATE`, and `DELETE` operations.

**Functionality** 

- The connector performs an initial sync of the tables.
  - This step can be skipped or modified by custom snapshot queries for each table. 
- The connector reads the binlog, produces change events for row-level `INSERT`, `UPDATE`, and `DELETE` operations.
- The schema changes are handled automatically without interruption and without needing a re-sync.
- You can choose to keep all changes in the resulting tables or deduplicate the batch result so that only the latest states of each record are reflected in the result table.
- The connector uses micro-batches, so you can schedule them as often as needed, whether every 5 minutes or twice a day.
  - This approach gives you more control over the resulting cost of the solution and lets you leverage the power of log-based replication even for smaller use cases if needed.
  - If your use case requires short interval syncs, reach out to us for information about a cost-efficient CDC add-on license.