# Delta Table with Structured Streaming

<!-- TOC -->
* [Delta Table with Structured Streaming](#delta-table-with-structured-streaming)
  * [Job trigger 1-min processing time](#job-trigger-1-min-processing-time)
    * [create streaming target table](#create-streaming-target-table)
    * [setup kafka streaming source and start streaming write to delta](#setup-kafka-streaming-source-and-start-streaming-write-to-delta)
    * [first micro-batch (minute 0-1)](#first-micro-batch-minute-0-1)
    * [second micro-batch (minute 1-2)](#second-micro-batch-minute-1-2)
    * [streaming query restart](#streaming-query-restart)
    * [read from streaming delta table](#read-from-streaming-delta-table)
    * [checkpoint cleanup](#checkpoint-cleanup)
    * [s3 versioning cleanup](#s3-versioning-cleanup)
<!-- TOC -->

Walk-through of how Delta tables change with Structured Streaming job.

## Job trigger 1-min processing time

### create streaming target table

```sql
CREATE TABLE streaming_events (
    event_id STRING,
    user_id BIGINT,
    event_type STRING,
    timestamp TIMESTAMP,
    event_date DATE
) USING DELTA
PARTITIONED BY (event_date)
```

```
S3 structure folder

streaming_events/
    _delta_log/
        00000000000000000000.json <- new (contains metadata)
```

### setup kafka streaming source and start streaming write to delta

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

kafka_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

kafka_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "user_events")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse JSON and add partition column
parsed_df = (kafka_df
    .select(from_json(col("value").cast("string"), kafka_schema).alias("data"))
    .select("data.*")
    .withColumn("event_date", to_date(col("timestamp")))
)

# Start streaming write
query = (parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://bucket/checkpoints/streaming_events")
    .trigger(processingTime="1 minute")
    .toTable("streaming_events")
    .start()
)
```

```
S3 structure folder

streaming_events/
    _delta_log/
        00000000000000000000.json

checkpoints/streaming_events/
    metadata <- streaming query metadata created on start

-----    
1. 00000000000000000000.json - Delta table creation metadata:
    - commitInfo with CREATE TABLE operation
    - metaData with schema including the new event_date partition column
    - protocol version information
2. checkpoints/streaming_events/metadata - spark query metadata:
    - Query ID and run ID
    - Initial state with batchId = -1 (before any batches)
    - Kafka source configuration
    - DeltaSink description
    - Empty metrics since no data has been processed yet
```

Sample content of 00000000000000000000.json
```json
{
  "commitInfo": {
    "timestamp": 1704096000000,
    "operation": "CREATE TABLE",
    "operationParameters": {
      "isManaged": "true",
      "description": null,
      "partitionBy": "[\"event_date\"]",
      "properties": "{}"
    },
    "readVersion": -1,
    "isolationLevel": "Serializable",
    "isBlindAppend": true,
    "operationMetrics": {},
    "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.0.0",
    "txnId": "550e8400-e29b-41d4-a716-446655440000"
  }
}
{
  "metaData": {
    "id": "550e8400-e29b-41d4-a716-446655440001",
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestamp\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": ["event_date"],
    "configuration": {},
    "createdTime": 1704096000000
  }
}
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  }
}
```

Sample content of checkpoints/streaming_events/metadata
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440013",
  "runId": "550e8400-e29b-41d4-a716-446655440014",
  "name": null,
  "timestamp": "2024-01-01T10:00:00.000Z",
  "batchId": -1,
  "batchDuration": 0,
  "durationMs": {
    "triggerExecution": 0
  },
  "eventTime": {},
  "stateOperators": [],
  "sources": [
    {
      "description": "KafkaV2[Subscribe[user_events]]",
      "startOffset": null,
      "endOffset": null,
      "numInputRows": 0,
      "inputRowsPerSecond": 0.0,
      "processedRowsPerSecond": 0.0
    }
  ],
  "sink": {
    "description": "DeltaSink[streaming_events]",
    "numOutputRows": -1
  }
}
```

### first micro-batch (minute 0-1)

Kafka messages processed:
```json
{"event_id": "evt_001", "user_id": 123, "event_type": "login", "timestamp": "2024-01-01T10:00:30.000Z"}
{"event_id": "evt_002", "user_id": 456, "event_type": "click", "timestamp": "2024-01-01T10:00:45.000Z"}
{"event_id": "evt_003", "user_id": 789, "event_type": "purchase", "timestamp": "2024-01-01T10:01:15.000Z"}
```

```
S3 structure folder

streaming_events/
    event_date=2024-01-01/
        part-00000-batch0-xxx.parquet <- new streaming data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json <- new (add operation from streaming)

checkpoints/streaming_events/
    metadata <- updated with batch 0 info
    sources/
        0/
            0 <- Kafka source state for source index 0
    state/
        0/
            0/ <- state store files (empty for stateless operations)
    commits/
        0 <- commit info for batch 0  
    offsets/
        0 <- Kafka starting offsets
        1 <- Kafka end offsets after processing batch 0 (3 partitions)
```

What gets created during first micro-batch:
1. Delta table data file (part-00000-batch0-xxx.parquet)
2. Delta transaction log entry (00000000000000000001.json) 
3. Spark streaming checkpoint structure (sources/, state/, commits/, offsets/)
4. Kafka offset tracking for exactly-once processing

Sample content of 00000000000000000001.json
```json
{
  "commitInfo": {
    "timestamp": 1704096030000,
    "operation": "STREAMING UPDATE",
    "operationParameters": {
      "outputMode": "Append",
      "queryId": "550e8400-e29b-41d4-a716-446655440013",
      "epochId": "0"
    },
    "readVersion": 0,
    "isolationLevel": "Serializable",
    "isBlindAppend": true,
    "operationMetrics": {
      "numAddedFiles": "1",
      "numOutputRows": "3",
      "numOutputBytes": "1456"
    },
    "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.0.0",
    "txnId": "550e8400-e29b-41d4-a716-446655440014"
  }
}
{
  "add": {
    "path": "event_date=2024-01-01/part-00000-batch0-xxx.parquet",
    "partitionValues": {
      "event_date": "2024-01-01"
    },
    "size": 1456,
    "modificationTime": 1704096030000,
    "dataChange": true,
    "stats": "{\"numRecords\":3,\"minValues\":{\"user_id\":123,\"timestamp\":\"2024-01-01T10:00:30.000Z\"},\"maxValues\":{\"user_id\":789,\"timestamp\":\"2024-01-01T10:01:15.000Z\"},\"nullCount\":{\"event_id\":0,\"user_id\":0,\"event_type\":0,\"timestamp\":0}}"
  }
}
```

Sample content of checkpoints/streaming_events/offsets/0
```json
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1704096000000,
  "conf": {
    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.join.stateFormatVersion": "2",
    "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion": "2",
    "spark.sql.streaming.multipleWatermarkPolicy": "min",
    "spark.sql.streaming.aggregation.stateFormatVersion": "2"
  }
}
{
  "user_events": {
    "0": 0,
    "1": 0,
    "2": 0
  }
}
```

Sample content of checkpoints/streaming_events/offsets/1  
```json
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1704096030000,
  "conf": {
    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.join.stateFormatVersion": "2",
    "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion": "2",
    "spark.sql.streaming.multipleWatermarkPolicy": "min",
    "spark.sql.streaming.aggregation.stateFormatVersion": "2"
  }
}
{
  "user_events": {
    "0": 2,
    "1": 1,
    "2": 0
  }
}
```

Sample content of checkpoints/streaming_events/commits/0
```json
{
  "nextBatchWatermarkMs": 0
}
```

Updated content of checkpoints/streaming_events/metadata (after batch 0)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440013",
  "runId": "550e8400-e29b-41d4-a716-446655440014",
  "name": null,
  "timestamp": "2024-01-01T10:01:00.000Z", <- updated to batch completion time
  "batchId": 0, <- changed from -1 to 0 (first actual batch)
  "batchDuration": 30000, <- now shows actual processing time
  "durationMs": { <- added detailed timing breakdown
    "addBatch": 25000,
    "getBatch": 3000,
    "queryPlanning": 1000,
    "triggerExecution": 30000,
    "walCommit": 1000
  },
  "eventTime": { <- added event time statistics from processed data
    "min": "2024-01-01T10:00:30.000Z",
    "max": "2024-01-01T10:01:15.000Z",
    "avg": "2024-01-01T10:00:52.500Z",
    "watermark": "1970-01-01T00:00:00.000Z"
  },
  "stateOperators": [],
  "sources": [
    {
      "description": "KafkaV2[Subscribe[user_events]]",
      "startOffset": { <- now shows actual Kafka starting offsets
        "user_events": {
          "0": 0,
          "1": 0,
          "2": 0
        }
      },
      "endOffset": { <- now shows actual Kafka ending offsets
        "user_events": {
          "0": 2,
          "1": 1,
          "2": 0
        }
      },
      "numInputRows": 3, <- changed from 0 to 3 (actual rows processed)
      "inputRowsPerSecond": 100.0, <- now shows actual throughput
      "processedRowsPerSecond": 100.0 <- now shows actual processing rate
    }
  ],
  "sink": {
    "description": "DeltaSink[streaming_events]",
    "numOutputRows": 3 <- changed from -1 to 3 (actual rows written)
  }
}
```

### second micro-batch (minute 1-2)

New Kafka messages processed (8 messages across 3 partitions):
```json
{"event_id": "evt_004", "user_id": 234, "event_type": "logout", "timestamp": "2024-01-01T10:01:30.000Z"}
{"event_id": "evt_005", "user_id": 567, "event_type": "view", "timestamp": "2024-01-01T10:02:00.000Z"}
{"event_id": "evt_006", "user_id": 890, "event_type": "click", "timestamp": "2024-01-01T10:02:15.000Z"}
{"event_id": "evt_007", "user_id": 345, "event_type": "purchase", "timestamp": "2024-01-01T10:02:30.000Z"}
{"event_id": "evt_008", "user_id": 678, "event_type": "login", "timestamp": "2024-01-01T10:02:45.000Z"}
{"event_id": "evt_009", "user_id": 456, "event_type": "view", "timestamp": "2024-01-01T10:03:00.000Z"}
{"event_id": "evt_010", "user_id": 123, "event_type": "logout", "timestamp": "2024-01-01T10:03:15.000Z"}
{"event_id": "evt_011", "user_id": 789, "event_type": "click", "timestamp": "2024-01-01T10:03:30.000Z"}
```

```
S3 structure folder

streaming_events/
    event_date=2024-01-01/
        part-00000-batch0-xxx.parquet
        part-00001-batch1-yyy.parquet <- new streaming data file
    _delta_log/
        00000000000000000000.json
        00000000000000000001.json
        00000000000000000002.json <- new (add operation from streaming)

checkpoints/streaming_events/
    metadata <- updated with batch 1 info
    sources/
        0/
            0
    state/
        0/
            0/
            1/ <- updated state store
    commits/
        0
        1 <- new commit for batch 1
    offsets/
        0
        1
        2 <- Kafka offsets after processing batch 1 (3 partitions)
```

Sample content of 00000000000000000002.json
```json
{
  "commitInfo": {
    "timestamp": 1704096090000,
    "operation": "STREAMING UPDATE",
    "operationParameters": {
      "outputMode": "Append",
      "queryId": "550e8400-e29b-41d4-a716-446655440013",
      "epochId": "1"
    },
    "readVersion": 1,
    "isolationLevel": "Serializable",
    "isBlindAppend": true,
    "operationMetrics": {
      "numAddedFiles": "1",
      "numOutputRows": "8",
      "numOutputBytes": "3891"
    },
    "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.0.0",
    "txnId": "550e8400-e29b-41d4-a716-446655440015"
  }
}
{
  "add": {
    "path": "event_date=2024-01-01/part-00001-batch1-yyy.parquet",
    "partitionValues": {
      "event_date": "2024-01-01"
    },
    "size": 3891,
    "modificationTime": 1704096090000,
    "dataChange": true,
    "stats": "{\"numRecords\":8,\"minValues\":{\"user_id\":123,\"timestamp\":\"2024-01-01T10:01:30.000Z\"},\"maxValues\":{\"user_id\":890,\"timestamp\":\"2024-01-01T10:03:30.000Z\"},\"nullCount\":{\"event_id\":0,\"user_id\":0,\"event_type\":0,\"timestamp\":0}}"
  }
}
```

Sample content of checkpoints/streaming_events/offsets/2
```json
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1704096090000,
  "conf": {
    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.join.stateFormatVersion": "2",
    "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion": "2",
    "spark.sql.streaming.multipleWatermarkPolicy": "min",
    "spark.sql.streaming.aggregation.stateFormatVersion": "2"
  }
}
{
  "user_events": {
    "0": 5, <- partition 0: processed 3 more messages (2 → 5)
    "1": 4, <- partition 1: processed 3 more messages (1 → 4) 
    "2": 2  <- partition 2: processed 2 more messages (0 → 2)
  }
}
```

Updated content of checkpoints/streaming_events/metadata (after batch 1)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440013",
  "runId": "550e8400-e29b-41d4-a716-446655440014",
  "name": null,
  "timestamp": "2024-01-01T10:03:30.000Z", <- updated to latest batch completion time
  "batchId": 1, <- incremented to batch 1
  "batchDuration": 60000, <- second batch took 60 seconds
  "durationMs": {
    "addBatch": 45000,
    "getBatch": 8000,
    "queryPlanning": 2000,
    "triggerExecution": 60000,
    "walCommit": 5000
  },
  "eventTime": {
    "min": "2024-01-01T10:01:30.000Z", <- updated to latest batch event time range
    "max": "2024-01-01T10:03:30.000Z",
    "avg": "2024-01-01T10:02:30.000Z",
    "watermark": "1970-01-01T00:00:00.000Z"
  },
  "stateOperators": [],
  "sources": [
    {
      "description": "KafkaV2[Subscribe[user_events]]",
      "startOffset": { <- starting offsets for batch 1
        "user_events": {
          "0": 2,
          "1": 1,
          "2": 0
        }
      },
      "endOffset": { <- ending offsets after batch 1
        "user_events": {
          "0": 5,
          "1": 4,
          "2": 2
        }
      },
      "numInputRows": 8, <- processed 8 rows in this batch
      "inputRowsPerSecond": 133.3, <- updated throughput metrics
      "processedRowsPerSecond": 133.3
    }
  ],
  "sink": {
    "description": "DeltaSink[streaming_events]",
    "numOutputRows": 8 <- wrote 8 rows to Delta table
  }
}
```

### streaming query restart

```python
# Stop the query
query.stop()

# Restart from checkpoint - automatically resumes from last processed offset
query_restart = (parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://bucket/checkpoints/streaming_events")
    .trigger(processingTime="1 minute")
    .toTable("streaming_events")
    .start()
)
```

Recovery process:
- Reads last committed Kafka offsets from `checkpoints/streaming_events/offsets/2`
- Resumes processing from next unprocessed messages across all 3 partitions
- Ensures exactly-once processing semantics

### read from streaming delta table

```python
# Stream changes from Delta table
delta_stream = (spark
    .readStream
    .format("delta")
    .table("streaming_events")
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    )
    .count()
)

# Output aggregated results
output_query = (delta_stream
    .writeStream
    .format("console")
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .start()
)
```

### checkpoint cleanup

```python
# Configure checkpoint cleanup (retain last 10 batches)
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "10")
```

```
S3 structure folder after cleanup

checkpoints/streaming_events/
    metadata
    sources/
        0/
            0
    state/
        0/
            93/ <- only recent state files retained
            94/
            95/
    commits/
        90 <- old commits cleaned up
        91
        92
        93
        94
        95
    offsets/
        90 <- old offsets cleaned up
        91
        92
        93
        94
        95
```

### s3 versioning cleanup

```
S3 bucket with versioning enabled

streaming_events/_delta_log/ (before cleanup):
    00000000000000000001.json (current)
    00000000000000000001.json?versionId=v1 (non-current)
    00000000000000000001.json?versionId=v2 (non-current)
    00000000000000000002.json (current)
    00000000000000000002.json?versionId=v1 (non-current)

checkpoints/streaming_events/commits/ (before cleanup):
    1 (current)
    1?versionId=v1 (non-current)
    1?versionId=v2 (non-current)
    2 (current)
    2?versionId=v1 (non-current)
```

S3 Lifecycle Policy:
```json
{
  "Rules": [
    {
      "ID": "DeltaStreamingCleanup",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "streaming_events/"
      },
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 7
      },
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 1
      }
    }
  ]
}
```

```
S3 bucket after lifecycle cleanup:

streaming_events/_delta_log/ (after cleanup):
    00000000000000000001.json (current only)
    00000000000000000002.json (current only)

checkpoints/streaming_events/commits/ (after cleanup):
    1 (current only)
    2 (current only)
```


