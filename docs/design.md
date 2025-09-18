# High-level design

## Overview

When running a sink connector with multiple sink tasks in Kafka Connect, each task will be assigned a subset of topic partitions to process at startup. From that point forward, tasks mostly operate independently from one another. They read from their assigned partitions, process messages, and commit Kafka offsets at a configured interval.

### Challenges

With the Iceberg sink connector, each tasks reads from Kafka and writes to data files. However, we want to avoid having every task perform the Iceberg commit to tables. This could lead to an (n * m) number of snapshots, where n is the number of tasks and m is the number of commit intervals. Excessive numbers of snapshots can lead to bloated metadata files and performance issues. This will also generate an excessive number of Iceberg catalog events.

Performing constant concurrent Iceberg commits to a table can result in contention and retries, which can impact performance and interfere with other jobs.

Finally, we want to correlate the Kafka consumer offsets with Iceberg commits, which we can use for exactly-once processing. However, we want to avoid storing all of the source offsets in an Iceberg snapshot, as a source topic may have many partition offsets.

### Features

* Commit coordination for centralized Iceberg commits
* Exactly-once delivery semantics
* Multi-table fan-out
* Row mutations (update/delete rows), upsert mode
* Automatic table creation and schema evolution
* Field name mapping via Iceberg’s column mapping functionality

![Iceberg%20KC](https://github.com/tabular-io/iceberg-kafka-connect/assets/5475421/edb8e698-0878-445f-8e41-140ba05c237e)

## Commit Coordinator

### Leader election

To get around those issues, we separate workers, which are responsible for writing the data files, and the coordinator, which is responsible for performing the Iceberg commit. At startup, a leader election is held to elect which worker will also be the coordinator. The simple algorithm we use is to elect the worker who is assigned partition 0 of the first topic defined in the configuration as the leader. This allows failover to another task upon partition reassignment if the original coordinator task fails.

### Communication channel

We use a control topic as the communication channel between the workers and the coordinator. This requires that the connector automatically create this topic, or the topic is created ahead of time. Using a topic for communication helps to decouple the workers, so for example a worker can send an event before the coordinator is available.

The control topic acts as a broadcast channel. All workers consume all events from the control topic, as does the coordinator. Coordinator-specific events are ignored by workers, and worker-specific events are ignored by the coordinator.

Workers always start reading from the latest offset in the control topic, so any events sent before startup will be ignored. The coordinator starts reading from the offset stored with the last successful commit, so if a worker sends an event before the coordinator starts, the coordinator will still receive it.

Control topic events are serialized using Avro to allow the schema to evolve over time while ensuring backwards compatibility. This aligns with Iceberg, which uses Avro to serialize many data structures.

## Message Processing

### Data file write

At startup, workers start processing messages and writing Iceberg data files using the Iceberg writer API. The destination table properties are used to determine writer properties such as format (Parquet, ORC, or Avro) and compression. Partition information is used to determine whether to use a simple writer or a partition fan-out writer. Write order and distribution are ignored.

### Conversion

The destination table schema is used as the source of truth. Messages are converted to Iceberg records by attempting to best fit them to the Iceberg schema. Fields are mapped by attempting to match any Iceberg name mappings. If there are no name mappings defined for the field then the field name in the Iceberg schema is used to match.

### Message handling

Every time a message comes in from the source topic, the worker or coordinator also checks the control topic for events. The control topic is also checked for events each time the sink task performs a flush. This ensures that events in the control topic are handled even if there is no incoming data in the source topic.

## Commits

### Iceberg commit

The coordinator initializes the commit timer at startup and checks if the configured commit interval has been reached each time it processes messages (or flush is called).

When the commit time interval has been reached, the coordinator initiates the commit process. It sends out a begin commit event which all workers consume. Each worker then finishes writing whatever was in progress. The worker collects all of the files that were written since the last commit, and sends that data in a data files event on the control topic. Then the worker initializes a new writer and continues writing data which will be included in the subsequent commit.

The coordinator consumes the data files events and checks that the responses cover all of the topic’s partitions. Once all data files events have been received, the coordinator performs the Iceberg commit. This involves collecting all of the data file objects that were created by each worker and committing an Iceberg table append or row delta. The offsets for the control topic at the time of the commit are set as a snapshot summary property. The offsets are also be committed to a consumer group if all commits are successful.

#### Snapshot properties

Every Iceberg commit performed by the sink includes some snapshot summary properties. As mentioned above, one such property is the control topic offsets. Another is the unique UUID assigned to every commit. Finally there is a VTTS (valid-through timestamp) property indicating through what timestamp records have been fully processed, i.e. all records processed from then on will have a timestamp greater than the VTTS. This is calculated by taking the maximum timestamp of records processed from each topic partition, and taking the minimum of these. If any partitions were not processed as part of the commit then the VTTS is not set.

### Offset commit

There are two sets of offsets to manage, the offsets for the source topic(s) and the offsets for the control topic.

#### Source topic

The offsets for the source topic are managed by the workers. A worker sends its data files events and also commits the source offsets to the Kafka Connect consumer group within a Kafka transaction. All control topic consumers have isolation set to read only committed events. This ensures that files sent to the coordinator correspond to the source topic offsets that were stored.

The Kafka Connect managed offsets are kept in sync during flushes. The reason behind having a second consumer group, rather than only using the Kafka Connect consumer group, is to ensure that the offsets are committed in a transaction with the sending of the data files events. The Kafka Connect consumer group cannot be directly updated as it has active consumers.

#### Control topic

On coordinator startup, the control topic offsets are restored from the consumer group. Any data files events added after the offsets are processed during startup. If the consumer group had not yet been initialized, then the coordinator’s consumer starts reading from the latest.

The control topic offsets are also stored in the Iceberg snapshot as a summary property. Before committing to a table, this property is read from the table. Only data files events with offsets after this value are committed to the table.

## Multi-table Fan-Out

It is possible to write to multiple Iceberg tables. Records are written to tables based on a routing configuration. This configuration defines which records should be written to a table based on a field value in the record. If no route configuration is defined then all records are written to all tables.

Each table stores the control topic offsets for the last successful commit. On coordinator startup, the control topic consumer’s offsets are initialized from the consumer group, which has the offsets from the last time there was a successful commit of all tables. This acts as a low watermark, and no events submitted before these offsets are reprocessed.

Before committing files to a table, the latest offsets stored for that table are retrieved. Any events with offsets less than those are skipped. This is to allow for recovery if a previous partial commit left some tables with different offsets than others.

## Row Mutations

Updates and deletes to a table are supported by configuring a field to use in the source message that maps to the type of operation to perform. The field value is mapped to an insert, update, or delete operation. An insert acts as an Iceberg append, a delete acts as an Iceberg equality delete, and an update acts as an equality delete followed by an append. This requires that an identity field (or fields) be defined in the table schema. This also requires that the destination table is Iceberg v2 format.

An upsert mode is also supported for data that is not in change data capture form. In this mode, all incoming records are treated as updates. For each record, an equality delete is performed followed by an append.

## Delivery Semantics

The connector has exactly-once semantics. Workers ensure this by sending the data files events and committing offsets for the source topic within a Kafka transaction. The coordinator ensures this by setting the control topic consumer to only read committed events, and also by saving the offsets for the control topic as part of the Iceberg commit data.

* The offsets for the source topic in the Kafka Connect consumer group correspond to the data files events successfully written to the control topic.
* The offsets for the control topic correspond to the Iceberg snapshot, as the offsets are stored in the snapshot metadata.

### Zombie fencing

If the task running the Coordinator process encounters a heavy GC cycle that causes a pause longer than the consumer session timeout (45 seconds by default), it may become a zombie. 
In this scenario, Kafka Connect will replace that task with a new one even though the “zombie” is still alive (but in a degraded state).
A new Coordinator process will begin processing datafiles from the control topic.
When the zombie starts processing again later, it may commit a datafile that has already been committed by the new Coordinator process, leading to duplicates in this extreme case. 
Coordinator zombie fencing will be targeted for a future release.

## Error Handling

All errors in the connector itself are non-retryable. This includes errors during commit coordination and message conversion. Errors that occur within Iceberg or Kafka Connect are handled internally to those libraries and may retry. Kafka Connect, by default, fails a task if it encounters a non-retryable error. Error tolerance and dead letter queue routing can be configured if desired.

## Recovery

### Worker fails during processing

If a failure occurs on a worker while processing messages or writing files, an exception is thrown and the task restarts from the last Kafka offsets committed to the Kafka Connect managed consumer group. Any data that had been written since the last commit is left in place, uncommitted. New data files are written from the offsets, and only these will be committed. Table maintenance should be performed regularly to clean up the orphaned files.

### Worker fails to receive begin commit event

If a worker fails at the time the coordinator sends out a begin commit event, it may not receive the request, as the worker only receives new events upon restart. In this case, the coordinator waits a specified amount of time. If this timeout is reached, the coordinator commits whatever it has received in its queue.

If a worker sends a data files event after the commit timeout then the files are included in the next commit.

### Coordinator fails while waiting for worker responses

If the coordinator fails while workers are still sending back commit responses, before committing anything to Iceberg, then the pending files buffered in memory are lost. To recover, when the coordinator starts up, it reads all data files events since the last table commit to retrieve the list of files that were submitted by workers but never committed. It includes these when the next commit is triggered.

### Coordinator fails to commit to Iceberg

If the Iceberg commit fails, then the commit buffer is left in place, and the coordinator’s control topic offsets are not saved. When the next commit interval is triggered, whatever remained in the commit buffer will be committed along with any new files.

### Table rolled back to older snapshot

If the table is rolled back to an older snapshot, then that also rolls back to older control topic offsets. For now, it is up to the user to manage how to proceed. If they do not want to re-commit the previously committed files, then they need to clear the control topic, e.g. delete it and recreate it. Also, if they want to roll back the Kafka offsets in the source topic, they need to set these offsets in the sink managed consumer group.

## Future Enhancements

* Optionally commit as unpartitioned to avoid many small files
* More seamless snapshot rollback behavior
* Pluggable commit coordinator
  * Allow a backend to handle instead of requiring a control topic
* Distribute commits across workers
* Coordinator zombie fencing

## Alternatives Considered

* Server-side commit coordination, where files are submitted to a server-side component (e.g. catalog service) and the server-side handles the Iceberg commit
  * Requiring a server-side component would limit who can use the connector
  * Rejected in favor of a more broadly useable approach
