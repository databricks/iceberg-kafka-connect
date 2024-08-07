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

import static java.util.stream.Collectors.groupingBy;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = new LinkedList<>();
  private final List<DataComplete> readyBuffer = new LinkedList<>();
  private long startTime;
  private UUID currentCommitId;
  private final IcebergSinkConfig config;

  private final Comparator<OffsetDateTime> dateTimeComparator =
          OffsetDateTime::compareTo;

  public CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  public void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitInProgress()) {
      LOG.debug(
          "Received data written with commit-id={} when no commit in progress, this can happen during recovery",
          ((DataWritten) envelope.event().payload()).commitId());
    }
  }

  public void addReady(Envelope envelope) {
    readyBuffer.add((DataComplete) envelope.event().payload());
    if (!isCommitInProgress()) {
      LOG.debug(
          "Received data complete for commit-id={} when no commit in progress, this can happen during recovery",
          ((DataComplete) envelope.event().payload()).commitId());
    }
  }

  public UUID currentCommitId() {
    return currentCommitId;
  }

  public boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  public boolean isCommitIntervalReached() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  public void startNewCommit() {
    currentCommitId = UUID.randomUUID();
    startTime = System.currentTimeMillis();
  }

  public void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  public void clearResponses() {
    commitBuffer.clear();
  }

  public boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    long currentTime = System.currentTimeMillis();
    if (currentTime - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached. Now: {}, start: {}, timeout: {}", currentTime, startTime, config.commitTimeoutMs());
      return true;
    }
    return false;
  }

  public boolean isCommitReady(int expectedPartitionCount) {
    if (!isCommitInProgress()) {
      return false;
    }

    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.commitId().equals(currentCommitId))
            .mapToInt(payload -> payload.assignments().size())
            .sum();

    if (receivedPartitionCount >= expectedPartitionCount) {
      LOG.info(
          "Commit {} ready, received responses for all {} partitions",
          currentCommitId,
          receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
        currentCommitId,
        receivedPartitionCount,
        expectedPartitionCount);

    return false;
  }

  public Map<TableIdentifier, List<Envelope>> tableCommitMap() {
    return commitBuffer.stream()
        .collect(
            groupingBy(
                envelope ->
                    ((DataWritten) envelope.event().payload())
                        .tableReference()
                        .identifier()));
  }

  public OffsetDateTime vtts(boolean partialCommit) {
    boolean validVtts =
        !partialCommit
            && readyBuffer.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

      OffsetDateTime result;
      if (validVtts) {
          Optional<OffsetDateTime> maybeResult =
                  readyBuffer.stream()
                          .flatMap(event -> event.assignments().stream())
                          .map(TopicPartitionOffset::timestamp)
                          .min(dateTimeComparator);
          if (maybeResult.isPresent()) {
              result = maybeResult.get();
          } else {
              throw new NoSuchElementException("no vtts found");
          }
    } else {
      result = null;
    }
    return result;
  }
}
