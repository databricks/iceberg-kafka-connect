/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.fixtures.EventTestUtil;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoordinatorTest extends ChannelTestBase {

  @Test
  public void testCommitAppend() {
    Assertions.assertEquals(0, ImmutableList.copyOf(table.snapshots().iterator()).size());

    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId =
        coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts);
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
        .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(
        Long.toString(ts.toInstant().toEpochMilli()), summary.get(VTTS_SNAPSHOT_PROP));
  }

  @Test
  public void testCommitDelta() {
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId =
        coordinatorTest(
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(EventTestUtil.createDeleteFile()),
            ts);

    assertThat(producer.history()).hasSize(3);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
        .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.OVERWRITE, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(
        Long.toString(ts.toInstant().toEpochMilli()), summary.get(VTTS_SNAPSHOT_PROP));
  }

  @Test
  public void testCommitNoFiles() {
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(2);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
        .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitComplete(1, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  @Test
  public void testCommitError() {
    // this spec isn't registered with the table
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();

    coordinatorTest(
        ImmutableList.of(badDataFile),
        ImmutableList.of(),
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(0L), ZoneOffset.UTC));

    // no commit messages sent
    assertThat(producer.history()).hasSize(1);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
        .isEqualTo(ImmutableMap.of());

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  @Test
  public void testShouldDeduplicateDataFilesBeforeAppending() {
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    DataFile dataFile = EventTestUtil.createDataFile();

    UUID commitId =
        coordinatorTest(
            currentCommitId -> {
              Event commitResponse =
                  new Event(
                      config.connectGroupId(),
                      new DataWritten(
                          StructType.of(),
                          currentCommitId,
                          new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                          ImmutableList.of(dataFile, dataFile), // duplicated data files
                          ImmutableList.of()));

              return ImmutableList.of(
                  commitResponse,
                  commitResponse, // duplicate commit response
                  new Event(
                      config.connectGroupId(),
                      new DataComplete(
                          currentCommitId,
                          ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)))));
            });

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());
  }

  @Test
  public void testShouldDeduplicateDeleteFilesBeforeAppending() {
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    DeleteFile deleteFile = EventTestUtil.createDeleteFile();

    UUID commitId =
        coordinatorTest(
            currentCommitId -> {
              Event duplicateCommitResponse =
                  new Event(
                      config.connectGroupId(),
                      new DataWritten(
                          StructType.of(),
                          currentCommitId,
                          new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                          ImmutableList.of(),
                          ImmutableList.of(deleteFile, deleteFile))); // duplicate delete files

              return ImmutableList.of(
                  duplicateCommitResponse,
                  duplicateCommitResponse, // duplicate commit response
                  new Event(
                      config.connectGroupId(),
                      new DataComplete(
                          currentCommitId,
                          ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)))));
            });

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.OVERWRITE, snapshot.operation());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());
  }

  private void validateAddedFiles(
      Snapshot snapshot, Set<String> expectedDataFilePaths, PartitionSpec expectedSpec) {
    final List<DataFile> addedDataFiles = ImmutableList.copyOf(snapshot.addedDataFiles(table.io()));
    final List<DeleteFile> addedDeleteFiles =
        ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io()));

    Assertions.assertEquals(
        expectedDataFilePaths,
        addedDataFiles.stream().map(ContentFile::path).collect(Collectors.toSet()));

    Assertions.assertEquals(
        ImmutableSet.of(expectedSpec.specId()),
        Stream.concat(addedDataFiles.stream(), addedDeleteFiles.stream())
            .map(ContentFile::specId)
            .collect(Collectors.toSet()));
  }

  /**
   *
   *
   * <ul>
   *   <li>Sets up an empty table with 2 partition specs
   *   <li>Starts a coordinator with 2 worker assignment each handling a different topic-partition
   *   <li>Sends a commit request to workers
   *   <li>Each worker writes datafiles with a different partition spec
   *   <li>The coordinator receives datafiles from both workers eventually and commits them to the
   *       table
   * </ul>
   */
  @Test
  public void testCommitMultiPartitionSpecAppendDataFiles() {
    final PartitionSpec spec1 = table.spec();
    assert spec1.isUnpartitioned();

    // evolve spec to partition by date
    final PartitionSpec partitionByDate = PartitionSpec.builderFor(SCHEMA).identity("date").build();
    table.updateSpec().addField(partitionByDate.fields().get(0).name()).commit();
    final PartitionSpec spec2 = table.spec();
    assert spec2.isPartitioned();

    // pretend we have two workers each handling 1 topic partition
    final List<MemberDescription> members = Lists.newArrayList();
    for (int i : ImmutableList.of(0, 1)) {
      members.add(
          new MemberDescription(
              "memberId" + i,
              "clientId" + i,
              "host" + i,
              new MemberAssignment(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, i)))));
    }

    final Coordinator coordinator = new Coordinator(catalog, config, members, clientFactory);
    initConsumer();

    // start a new commit immediately and wait for all workers to respond infinitely
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    coordinator.process();

    // retrieve commitId from commit request produced by coordinator
    final byte[] bytes = producer.history().get(0).value();
    final Event commitRequest = AvroUtil.decode(bytes);
    assert commitRequest.type().equals(PayloadType.START_COMMIT);
    final UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    // each worker sends its responses for the commit request
    Map<Integer, PartitionSpec> workerIdToSpecMap =
        ImmutableMap.of(
            1, spec1, // worker 1 produces datafiles with the old partition spec
            2, spec2 // worker 2 produces datafiles with the new partition spec
            );

    int currentControlTopicOffset = 1;
    for (Map.Entry<Integer, PartitionSpec> entry : workerIdToSpecMap.entrySet()) {
      Integer workerId = entry.getKey();
      PartitionSpec spec = entry.getValue();

      final DataFile dataFile =
          DataFiles.builder(spec)
              .withPath(String.format("file%d.parquet", workerId))
              .withFileSizeInBytes(100)
              .withRecordCount(5)
              .build();

      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME,
              0,
              currentControlTopicOffset,
              "key",
              AvroUtil.encode(
                  new Event(
                      config.connectGroupId(),
                      new DataWritten(
                          spec.partitionType(),
                          commitId,
                          TableReference.of("catalog", TABLE_IDENTIFIER),
                          ImmutableList.of(dataFile),
                          ImmutableList.of())))));
      currentControlTopicOffset += 1;

      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME,
              0,
              currentControlTopicOffset,
              "key",
              AvroUtil.encode(
                  new Event(
                      config.connectGroupId(),
                      new DataComplete(
                          commitId,
                          ImmutableList.of(
                              new TopicPartitionOffset(
                                  SRC_TOPIC_NAME,
                                  0,
                                  100L,
                                  OffsetDateTime.ofInstant(
                                      Instant.ofEpochMilli(100L), ZoneOffset.UTC))))))));
      currentControlTopicOffset += 1;
    }

    // all workers have responded so coordinator can process responses now
    coordinator.process();

    // assertions
    table.refresh();
    final List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(2, snapshots.size(), "Expected 2 snapshots, one for each spec.");

    final Snapshot firstSnapshot = snapshots.get(0);
    final Snapshot secondSnapshot = snapshots.get(1);

    validateAddedFiles(firstSnapshot, ImmutableSet.of("file1.parquet"), spec1);
    validateAddedFiles(secondSnapshot, ImmutableSet.of("file2.parquet"), spec2);

    Assertions.assertEquals(
        commitId.toString(),
        firstSnapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP),
        "All snapshots should be tagged with a commit-id");
    Assertions.assertNull(
        firstSnapshot.summary().getOrDefault(OFFSETS_SNAPSHOT_PROP, null),
        "Earlier snapshots should not include control-topic-offsets in their summary");
    Assertions.assertNull(
        firstSnapshot.summary().getOrDefault(VTTS_SNAPSHOT_PROP, null),
        "Earlier snapshots should not include vtts in their summary");

    Assertions.assertEquals(
        commitId.toString(),
        secondSnapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP),
        "All snapshots should be tagged with a commit-id");
    Assertions.assertEquals(
        "{\"0\":5}",
        secondSnapshot.summary().get(OFFSETS_SNAPSHOT_PROP),
        "Only the most recent snapshot should include control-topic-offsets in it's summary");
    Assertions.assertEquals(
        "100",
        secondSnapshot.summary().get(VTTS_SNAPSHOT_PROP),
        "Only the most recent snapshot should include vtts in it's summary");
  }

  private void assertCommitTable(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitTable = AvroUtil.decode(bytes);
    assertThat(commitTable.type()).isEqualTo(PayloadType.COMMIT_TO_TABLE);
    CommitToTable commitTablePayload = (CommitToTable) commitTable.payload();
    assertThat(commitTablePayload.commitId()).isEqualTo(commitId);
    assertThat(commitTablePayload.tableReference().identifier().toString())
        .isEqualTo(TABLE_IDENTIFIER.toString());
    assertThat(commitTablePayload.validThroughTs()).isEqualTo(ts);
  }

  private void assertCommitComplete(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitComplete = AvroUtil.decode(bytes);
    assertThat(commitComplete.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    CommitComplete commitCompletePayload = (CommitComplete) commitComplete.payload();
    assertThat(commitCompletePayload.commitId()).isEqualTo(commitId);
    assertThat(commitCompletePayload.validThroughTs()).isEqualTo(ts);
  }

  private UUID coordinatorTest(
      List<DataFile> dataFiles, List<DeleteFile> deleteFiles, OffsetDateTime ts) {
    return coordinatorTest(
        currentCommitId -> {
          Event commitResponse =
              new Event(
                  config.connectGroupId(),
                  new DataWritten(
                      StructType.of(),
                      currentCommitId,
                      new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                      dataFiles,
                      deleteFiles));

          Event commitReady =
              new Event(
                  config.connectGroupId(),
                  new DataComplete(
                      currentCommitId,
                      ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));

          return ImmutableList.of(commitResponse, commitReady);
        });
  }

  private UUID coordinatorTest(Function<UUID, List<Event>> eventsFn) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(), clientFactory);

    // init consumer after subscribe()
    initConsumer();

    coordinator.process();

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.history()).hasSize(1);

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = AvroUtil.decode(bytes);
    assertThat(commitRequest.type()).isEqualTo(PayloadType.START_COMMIT);

    UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    int currentOffset = 1;
    for (Event event : eventsFn.apply(commitId)) {
      bytes = AvroUtil.encode(event);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, currentOffset, "key", bytes));
      currentOffset += 1;
    }

    when(config.commitIntervalMs()).thenReturn(0);

    coordinator.process();

    return commitId;
  }
}
