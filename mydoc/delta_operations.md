The main operation types:

Write Operations:

- WRITE - Regular batch write (INSERT, CREATE TABLE AS SELECT)
- STREAMING UPDATE - Structured Streaming write
- MERGE - MERGE INTO operations (upserts)
- DELETE - DELETE operations
- UPDATE - UPDATE operations
- RESTORE - RESTORE TABLE operations

Schema Operations:

- CREATE TABLE - Table creation
- ADD COLUMNS - ALTER TABLE ADD COLUMN
- CHANGE COLUMN - ALTER TABLE ALTER COLUMN
- DROP COLUMNS - ALTER TABLE DROP COLUMN
- SET TBLPROPERTIES - ALTER TABLE SET TBLPROPERTIES
- UNSET TBLPROPERTIES - ALTER TABLE UNSET TBLPROPERTIES

Maintenance Operations:

- OPTIMIZE - OPTIMIZE TABLE operations
- VACUUM - VACUUM operations
- FSCK - File system consistency check

Advanced Operations:

- CLONE - CLONE TABLE operations
- CONVERT - CONVERT TO DELTA operations
- REORG TABLE - Table reorganization

STREAMING UPDATE vs Regular WRITE:

| Aspect        | STREAMING UPDATE     | WRITE                |
|---------------|----------------------|----------------------|
| Trigger       | Structured Streaming | Batch operations     |
| Frequency     | High (every trigger) | Low (on-demand)      |
| Size          | Small micro-batches  | Large batches        |
| Metadata      | queryId, epochId     | Operation parameters |
| Mode          | Append-only usually  | Insert/Overwrite     |
| Checkpointing | Integrated           | Not applicable       |

```javascript
STREAMING UPDATE:
{
    "operation": "STREAMING UPDATE",
    "operationParameters": {
      "outputMode": "Append",
      "queryId": "550e8400-e29b-41d4-a716-446655440013",
      "epochId": "0"
    }
}

Regular WRITE:
{
    "operation": "WRITE",
    "operationParameters": {
      "mode": "Overwrite",
      "partitionBy": "[\"partition_date\"]"
    }
}
```