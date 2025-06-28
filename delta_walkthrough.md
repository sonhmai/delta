# Delta Table Understanding Walkthrough

<!-- TOC -->
* [Delta Table Understanding Walkthrough](#delta-table-understanding-walkthrough)
  * [Abstract](#abstract)
  * [Daily Snapshot Table](#daily-snapshot-table)
    * [create](#create)
    * [first write of date T](#first-write-of-date-t)
    * [read](#read-)
    * [second write of date T+1](#second-write-of-date-t1)
    * [read](#read)
    * [third write of date T+2](#third-write-of-date-t2)
    * [overwrite of date T+2 to fix data quality](#overwrite-of-date-t2-to-fix-data-quality)
    * [read T+2](#read-t2)
    * [overwrite of date T to fix data quality](#overwrite-of-date-t-to-fix-data-quality)
    * [read T](#read-t)
    * [time travel to rollback T to a previous version](#time-travel-to-rollback-t-to-a-previous-version)
    * [write T+3](#write-t3)
    * [vacuum](#vacuum)
    * [write T+4](#write-t4)
    * [optimize](#optimize)
    * [write T+5](#write-t5)
    * [checkpoint](#checkpoint)
  * [Schema Evolution Examples](#schema-evolution-examples)
    * [add new column](#add-new-column)
    * [write with new schema](#write-with-new-schema)
    * [read with mixed schemas](#read-with-mixed-schemas)
    * [change column type](#change-column-type)
    * [drop column](#drop-column)
    * [read after column drop](#read-after-column-drop)
    * [rewrite with new schema](#rewrite-with-new-schema)
  * [Additional Improvements](#additional-improvements)
<!-- TOC -->

## Abstract
A walkthrough to what happens to delta table and the transaction log 
when we do something to it.

This is to build an intuition of how delta tables work and how the transaction log is structured.

## Daily Snapshot Table

### create

``` 
CREATE TABLE daily_snapshot (
    id bigint,
    name STRING,
    feature1 double
) USING DELTA
PARTITIONED BY (partition_date DATE)
```

``` 
S3 structure folder

daily_snapshot/
    _delta_log/
        00000000000000000000.json <- new (contains metadata)
```

### first write of date T

```sql
INSERT INTO daily_snapshot 
VALUES (1, 'alice', 0.5, '2024-01-01'),
       (2, 'bob', 0.8, '2024-01-01')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00000-xxx.parquet <- new data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json <- new (add operation)
```

### read 

```sql
SELECT * FROM daily_snapshot WHERE partition_date = '2024-01-01'
```

Delta reads the transaction log to discover files:
- Reads 00000000000000000001.json to find part-00000-xxx.parquet
- Returns 2 rows

### second write of date T+1

```sql
INSERT INTO daily_snapshot 
VALUES (3, 'charlie', 0.3, '2024-01-02'),
       (4, 'diana', 0.9, '2024-01-02')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00000-xxx.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet <- new data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json <- new (add operation)
```

### read

```sql
SELECT * FROM daily_snapshot
```

Delta reads transaction log sequentially:
- Discovers 2 parquet files from logs 001 and 002
- Returns 4 rows total

### third write of date T+2

```sql
INSERT INTO daily_snapshot 
VALUES (5, 'eve', 0.7, '2024-01-03'),
       (6, 'frank', 0.4, '2024-01-03')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00000-xxx.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00002-zzz.parquet <- new data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json <- new (add operation)
```

### overwrite of date T+2 to fix data quality

```sql
INSERT OVERWRITE daily_snapshot 
PARTITION (partition_date = '2024-01-03')
VALUES (5, 'eve_corrected', 0.75, '2024-01-03'),
       (6, 'frank_corrected', 0.45, '2024-01-03')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00000-xxx.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00002-zzz.parquet <- still exists but logically removed
        part-00003-aaa.parquet <- new corrected data
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json <- new (remove + add operations)
```

### read T+2

```sql
SELECT * FROM daily_snapshot WHERE partition_date = '2024-01-03'
```

Delta reads transaction log and sees:
- Log 003: add part-00002-zzz.parquet
- Log 004: remove part-00002-zzz.parquet, add part-00003-aaa.parquet
- Returns corrected data from part-00003-aaa.parquet

### overwrite of date T to fix data quality

```sql
INSERT OVERWRITE daily_snapshot 
PARTITION (partition_date = '2024-01-01')
VALUES (1, 'alice_updated', 0.55, '2024-01-01'),
       (2, 'bob_updated', 0.85, '2024-01-01')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00000-xxx.parquet <- still exists but logically removed
        part-00004-bbb.parquet <- new corrected data
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00002-zzz.parquet
        part-00003-aaa.parquet
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json <- new (remove + add operations)
```

### read T

```sql
SELECT * FROM daily_snapshot WHERE partition_date = '2024-01-01'
```

Returns updated data from part-00004-bbb.parquet

### time travel to rollback T to a previous version

```sql
SELECT * FROM daily_snapshot VERSION AS OF 1 WHERE partition_date = '2024-01-01'
```

Delta reads transaction log up to version 1:
- Ignores changes in logs 004 and 005
- Returns original data from part-00000-xxx.parquet

### write T+3

```sql
INSERT INTO daily_snapshot 
VALUES (7, 'grace', 0.6, '2024-01-04')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00000-xxx.parquet
        part-00004-bbb.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00002-zzz.parquet
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet <- new data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json <- new (add operation)
```

### vacuum

```sql
VACUUM daily_snapshot RETAIN 0 HOURS
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet <- only current files remain
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet <- old removed files deleted
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json <- still ID 6, no new log entry for vacuum
```

### write T+4

```sql
INSERT INTO daily_snapshot 
VALUES (8, 'henry', 0.2, '2024-01-05'),
       (9, 'iris', 0.9, '2024-01-05')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    partition_date=2024-01-05/
        part-00006-ddd.parquet <- new data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json
        00000000000000000007.json <- new (add operation)
```

### optimize

```sql
OPTIMIZE daily_snapshot
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet <- may be rewritten/compacted
        part-00007-optimized.parquet <- new optimized file
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    partition_date=2024-01-05/
        part-00006-ddd.parquet
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json
        00000000000000000007.json
        00000000000000000008.json <- new (remove + add for optimized files)
```

### write T+5

```sql
INSERT INTO daily_snapshot 
VALUES (10, 'jack', 0.8, '2024-01-06')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet
        part-00007-optimized.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    partition_date=2024-01-05/
        part-00006-ddd.parquet
    partition_date=2024-01-06/
        part-00008-eee.parquet <- new data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json
        00000000000000000007.json
        00000000000000000008.json
        00000000000000000009.json <- new (add operation)
```

### checkpoint

```sql
-- Checkpoint automatically created at log 010
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet
        part-00007-optimized.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    partition_date=2024-01-05/
        part-00006-ddd.parquet
    partition_date=2024-01-06/
        part-00008-eee.parquet
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json
        00000000000000000007.json
        00000000000000000008.json
        00000000000000000009.json
        00000000000000000010.checkpoint.parquet <- checkpoint file
        _last_checkpoint <- points to checkpoint 010
```

## Schema Evolution Examples

### add new column

```sql
ALTER TABLE daily_snapshot ADD COLUMN feature2 double
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet <- old files unchanged
        part-00007-optimized.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    partition_date=2024-01-05/
        part-00006-ddd.parquet
    partition_date=2024-01-06/
        part-00008-eee.parquet
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json
        00000000000000000007.json
        00000000000000000008.json
        00000000000000000009.json
        00000000000000000010.checkpoint.parquet
        00000000000000000011.json <- new (metaData action with updated schema)
        _last_checkpoint
```

### write with new schema

```sql
INSERT INTO daily_snapshot 
VALUES (11, 'kelly', 0.3, 0.7, '2024-01-07')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet <- old schema: id, name, feature1, partition_date
        part-00007-optimized.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    partition_date=2024-01-05/
        part-00006-ddd.parquet
    partition_date=2024-01-06/
        part-00008-eee.parquet
    partition_date=2024-01-07/
        part-00009-fff.parquet <- new schema: id, name, feature1, feature2, partition_date
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json
        00000000000000000003.json
        00000000000000000004.json
        00000000000000000005.json
        00000000000000000006.json
        00000000000000000007.json
        00000000000000000008.json
        00000000000000000009.json
        00000000000000000010.checkpoint.parquet
        00000000000000000011.json
        00000000000000000012.json <- new (add operation)
```

### read with mixed schemas

```sql
SELECT * FROM daily_snapshot
```

Delta handles schema evolution:
- Reads old files with original schema (feature2 = null)
- Reads new files with extended schema
- Returns unified result with null values for missing columns

### change column type

```sql
ALTER TABLE daily_snapshot ALTER COLUMN feature1 TYPE decimal(10,2)
```

```
S3 structure folder

daily_snapshot/
    [same files as before]
    _delta_log/
        [previous logs...]
        00000000000000000013.json <- new (metaData action with type change)
```

### drop column

```sql
ALTER TABLE daily_snapshot DROP COLUMN feature2
```

```
S3 structure folder

daily_snapshot/
    [same files as before - data still exists physically]
    _delta_log/
        [previous logs...]
        00000000000000000014.json <- new (metaData action with dropped column)
```

### read after column drop

```sql
SELECT * FROM daily_snapshot
```

Delta ignores dropped columns:
- Reads all files but excludes feature2 column from results
- Physical files still contain feature2 data until rewritten

### rewrite with new schema

```sql
INSERT OVERWRITE daily_snapshot 
PARTITION (partition_date = '2024-01-07')
VALUES (11, 'kelly_updated', 0.35, '2024-01-07')
```

```
S3 structure folder

daily_snapshot/
    partition_date=2024-01-01/
        part-00004-bbb.parquet <- still has old schema
        part-00007-optimized.parquet
    partition_date=2024-01-02/
        part-00001-yyy.parquet
    partition_date=2024-01-03/
        part-00003-aaa.parquet
    partition_date=2024-01-04/
        part-00005-ccc.parquet
    partition_date=2024-01-05/
        part-00006-ddd.parquet
    partition_date=2024-01-06/
        part-00008-eee.parquet
    partition_date=2024-01-07/
        part-00009-fff.parquet <- logically removed
        part-00010-ggg.parquet <- new file with current schema (no feature2)
    _delta_log/
        [previous logs...]
        00000000000000000015.json <- new (remove + add operations)
```

## Additional Improvements

2. **Include file statistics** - Show how Delta tracks row counts, min/max values in transaction logs
3. **Demonstrate concurrent writes** - Show how Delta handles conflicts and retries
4. **Add Z-order optimization** - Show how ZORDER BY affects file layout
5. **Include table properties** - Show how to set retention policies, auto-optimize, etc.