# Mongo-Transit
This is a tool for automatic replication of documents from one MongoDB cluster to another.

# Configurations

## CLI arguments

- `-s`, `--source`: Connection string for source server or cluster. **Required**.
- `-d`, `--destination`: Connection string for destination server or cluster. **Required**.
- `-c`, `--config`: YAML file with configuration. **Required**.
- `-r`, `--runs`: How many transition cycles should tool do. Zero value will result in infinite cycle. *Default: 0.*
- `--verbose`: Log debug information into console.
- `--dry`: Run tool without inserting or updating records.
- `--logs`: Directory for storing logs. By default they will be stored in current directory.
- `-n`, `--notify`: Notification interval in seconds. *Default: 3.*
- `-w`, `--workers`: Degree of parallelization for inserts, value is amount per each CPU (core). *Default: 4.*
- `-b`, `--batchSize`: Batch size for insertion. *Default: 1000.*

## Config file

YAML config file contains array of settings for each collection to be transferred.

Example:
```yaml
- name: TestCollection
  database: Test
  iterativeOptions:
    field: LastModified
    forcedCheckpoint: 2021-03-15T16:00:00.894Z
    offsetInMinutes: 60
  shardKey:
    fields:
    - UserId
    - _id
    fromDestination: false
  noUpsert: false
```

Description:
- `name`: collection name.
- `database`: database name.
- `iterativeOptions`: options for iterative transfer, i.e. when tool transfer data only from last known point-in-time.
    - `field`: name of the field, which contains checkpoint (version) value. *_Currently only UTC date values are supported._*
    - `forcedCheckpoint`: option for making a transfer from specified point in time.
    - `offsetInMinutes`: time offset that is subtracted from last checkpoint value.
- `shardKey`: options dedicated to work with sharded cluster.
  - `fields`: an array of keys, that should be included in update filter. Useful when you restore collection with some unique index or a sharded collection.
  - `fromDestination`: when performing an upsert, load values for update filter not from source, but from destination. Useful when you are changing you shard key value - if not specified, it will cause duplicate key exception, cause it won't find document with new keys and perform insertion. **WARNING: currently this behavior is not supported.**
- `noUpsert`: disable upsert and perform regular update.