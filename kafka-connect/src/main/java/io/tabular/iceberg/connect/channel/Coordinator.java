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

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitCompletePayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.CommitTablePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.kafka.clients.admin.MemberDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OFFSETS_SNAPSHOT_PROP_FMT = "kafka.connect.offsets.%s.%s";
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;

  public Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.controlGroupId() + "-coord", config, clientFactory);

    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
        String.format(OFFSETS_SNAPSHOT_PROP_FMT, config.controlTopic(), config.controlGroupId());
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
    this.commitState = new CommitState(config);
  }

  public void process() {
    if (commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      Event event =
          new Event(
              config.controlGroupId(),
              EventType.COMMIT_REQUEST,
              new CommitRequestPayload(commitState.currentCommitId()));
      send(event);
    }

    consumeAvailable(POLL_DURATION);

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  @Override
  protected boolean receive(Envelope envelope) {
    switch (envelope.event().type()) {
      case COMMIT_RESPONSE:
        commitState.addResponse(envelope);
        return true;
      case COMMIT_READY:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableIdentifier, List<Envelope>> commitMap = commitState.tableCommitMap();

    String offsetsJson = offsetsJson();
    Long vtts = commitState.vtts(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              commitToTable(entry.getKey(), entry.getValue(), offsetsJson, vtts);
            });

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(
            config.controlGroupId(),
            EventType.COMMIT_COMPLETE,
            new CommitCompletePayload(commitState.currentCommitId(), vtts));
    send(event);

    LOG.info(
        "Commit {} complete, committed to {} table(s), vtts {}",
        commitState.currentCommitId(),
        commitMap.size(),
        vtts);
  }

  private String offsetsJson() {
    try {
      return MAPPER.writeValueAsString(controlTopicOffsets());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void commitToTable(
      TableIdentifier tableIdentifier, List<Envelope> envelopeList, String offsetsJson, Long vtts) {
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier);
      return;
    }

    Optional<String> branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch.orElse(null));

    List<CommitResponsePayload> payloads =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> (CommitResponsePayload) envelope.event().payload())
            .collect(toList());

    List<DataFile> dataFiles =
        payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());

    List<DeleteFile> deleteFiles =
        payloads.stream()
            .filter(payload -> payload.deleteFiles() != null)
            .flatMap(payload -> payload.deleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .collect(toList());

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
    } else {
      if (deleteFiles.isEmpty()) {
        AppendFiles appendOp = table.newAppend();
        branch.ifPresent(appendOp::toBranch);
        appendOp.set(snapshotOffsetsProp, offsetsJson);
        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        if (vtts != null) {
          appendOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts));
        }
        dataFiles.forEach(appendOp::appendFile);
        appendOp.commit();
      } else {
        RowDelta deltaOp = table.newRowDelta();
        branch.ifPresent(deltaOp::toBranch);
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        if (vtts != null) {
          deltaOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts));
        }
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }

      Long snapshotId = latestSnapshot(table, branch.orElse(null)).snapshotId();
      Event event =
          new Event(
              config.controlGroupId(),
              EventType.COMMIT_TABLE,
              new CommitTablePayload(
                  commitState.currentCommitId(), TableName.of(tableIdentifier), snapshotId, vtts));
      send(event);

      LOG.info(
          "Commit complete to table {}, snapshot {}, commit ID {}, vtts {}",
          tableIdentifier,
          snapshotId,
          commitState.currentCommitId(),
          vtts);
    }
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(snapshotOffsetsProp);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
        try {
          return MAPPER.readValue(value, typeRef);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return ImmutableMap.of();
  }
}
