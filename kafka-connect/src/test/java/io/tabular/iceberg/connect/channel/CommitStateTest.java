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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import io.tabular.iceberg.connect.events.TopicPartitionTransaction;
import io.tabular.iceberg.connect.events.TransactionDataComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class CommitStateTest {

  private OffsetDateTime offsetDateTime(Long ts) {
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC);
  }

  @Test
  public void testIsCommitReady() {
    TopicPartitionOffset tp = mock(TopicPartitionOffset.class);
    TopicPartitionTransaction tpt = mock(TopicPartitionTransaction.class);

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    TransactionDataComplete payload1 = mock(TransactionDataComplete.class);
    when(payload1.commitId()).thenReturn(commitState.currentCommitId());
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp, tp));
    when(payload1.txIds()).thenReturn(ImmutableList.of(tpt, tpt));

    TransactionDataComplete payload2 = mock(TransactionDataComplete.class);
    when(payload2.commitId()).thenReturn(commitState.currentCommitId());
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp));
    when(payload2.txIds()).thenReturn(ImmutableList.of(tpt));

    TransactionDataComplete payload3 = mock(TransactionDataComplete.class);
    when(payload3.commitId()).thenReturn(UUID.randomUUID());
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp));
    when(payload3.txIds()).thenReturn(ImmutableList.of(tpt));

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));
    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.isCommitReady(3)).isTrue();
    assertThat(commitState.isCommitReady(4)).isFalse();
  }

  @Test
  public void testGetVtts() {
    TransactionDataComplete payload1 = mock(TransactionDataComplete.class);
    TopicPartitionOffset tp1 = mock(TopicPartitionOffset.class);
    when(tp1.timestamp()).thenReturn(offsetDateTime(3L));
    TopicPartitionOffset tp2 = mock(TopicPartitionOffset.class);
    when(tp2.timestamp()).thenReturn(offsetDateTime(2L));
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp1, tp2));

    TransactionDataComplete payload2 = mock(TransactionDataComplete.class);
    TopicPartitionOffset tp3 = mock(TopicPartitionOffset.class);
    when(tp3.timestamp()).thenReturn(offsetDateTime(1L));
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp3));

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));

    assertThat(commitState.vtts(false)).isEqualTo(offsetDateTime(1L));
    assertThat(commitState.vtts(true)).isNull();

    // null timestamp for one, so should not set a vtts
    TransactionDataComplete payload3 = mock(TransactionDataComplete.class);
    TopicPartitionOffset tp4 = mock(TopicPartitionOffset.class);
    when(tp4.timestamp()).thenReturn(null);
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp4));

    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.vtts(false)).isNull();
    assertThat(commitState.vtts(true)).isNull();
  }

  private Envelope wrapInEnvelope(Payload payload) {
    Event event = mock(Event.class);
    when(event.payload()).thenReturn(payload);
    return new Envelope(event, 0, 0);
  }
}
