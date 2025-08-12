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

import io.tabular.iceberg.connect.data.SchemaUtils;
import io.tabular.iceberg.connect.events.CommitCompletePayload;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.CommitTablePayload;
import io.tabular.iceberg.connect.events.EventTestUtil;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import io.tabular.iceberg.connect.events.TopicPartitionTxId;
import io.tabular.iceberg.connect.events.TransactionDataComplete;
import org.apache.avro.Schema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class EventDecoderTest {
  private final UUID commitId = UUID.fromString("e0bf874a-0f83-4242-97c9-f0a7b5cb3f45");
  private final String catalogName = "catalog";

  private final EventDecoder eventDecoder = new EventDecoder(catalogName);

  @Test
  public void testCommitRequestBecomesStartCommit() {
    io.tabular.iceberg.connect.events.Event event =
        new io.tabular.iceberg.connect.events.Event(
            "cg-connector",
            io.tabular.iceberg.connect.events.EventType.COMMIT_REQUEST,
            new CommitRequestPayload(commitId));

    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);

    assertThat(result.groupId()).isEqualTo("cg-connector");
    assertThat(result.type()).isEqualTo(PayloadType.START_COMMIT);
    assertThat(result.payload()).isInstanceOf(StartCommit.class);
    StartCommit payload = (StartCommit) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
  }

  @Test
  public void testCommitResponseBecomesDataWrittenUnpartitioned() {
    io.tabular.iceberg.connect.events.Event event =
        new io.tabular.iceberg.connect.events.Event(
            "cg-connector",
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                Types.StructType.of(),
                commitId,
                new TableName(Collections.singletonList("db"), "tbl"),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);

    assertThat(result.groupId()).isEqualTo("cg-connector");
    assertThat(result.type()).isEqualTo(PayloadType.DATA_WRITTEN);
    assertThat(result.payload()).isInstanceOf(DataWritten.class);
    DataWritten payload = (DataWritten) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.dataFiles().size()).isEqualTo(2);
    assertThat(payload.deleteFiles().size()).isEqualTo(2);
    assertThat(payload.tableReference().catalog()).isEqualTo("catalog");
    assertThat(payload.tableReference().identifier()).isEqualTo(TableIdentifier.of("db", "tbl"));
    // should have an empty partition spec on the schema
    Schema.Field field =
        payload.getSchema().getFields().get(2).schema().getTypes().stream()
            .filter(s -> s.getType() != Schema.Type.NULL)
            .findFirst()
            .get()
            .getElementType()
            .getField("partition");
    assertThat(field.schema().getFields()).isEmpty();
  }

  @Test
  public void testCommitResponseBecomesDataWrittenPartitioned() {

    org.apache.iceberg.Schema schemaSpec =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "i", Types.IntegerType.get()),
            Types.NestedField.required(2, "s", Types.StringType.get()),
            Types.NestedField.required(3, "ts1", Types.TimestampType.withZone()),
            Types.NestedField.required(4, "ts2", Types.TimestampType.withZone()),
            Types.NestedField.required(5, "ts3", Types.TimestampType.withZone()),
            Types.NestedField.required(6, "ts4", Types.TimestampType.withZone()));

    List<String> partitionFields =
        ImmutableList.of(
            "year(ts1)",
            "month(ts2)",
            "day(ts3)",
            "hour(ts4)",
            "bucket(i, 4)",
            "truncate(s, 10)",
            "s");
    PartitionSpec spec = SchemaUtils.createPartitionSpec(schemaSpec, partitionFields);

    Types.StructType structType = spec.partitionType();

    io.tabular.iceberg.connect.events.Event event =
        new io.tabular.iceberg.connect.events.Event(
            "cg-connector",
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                structType,
                commitId,
                new TableName(Collections.singletonList("db"), "tbl"),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);

    assertThat(event.groupId()).isEqualTo("cg-connector");
    assertThat(result.type()).isEqualTo(PayloadType.DATA_WRITTEN);
    assertThat(result.payload()).isInstanceOf(DataWritten.class);
    DataWritten payload = (DataWritten) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.dataFiles().size()).isEqualTo(2);
    assertThat(payload.deleteFiles().size()).isEqualTo(2);
    assertThat(payload.tableReference().catalog()).isEqualTo("catalog");
    assertThat(payload.tableReference().identifier()).isEqualTo(TableIdentifier.of("db", "tbl"));

    assertThat(payload.writeSchema()).isEqualTo(
            Types.StructType.of(
                    Types.NestedField.required(10_300, "commit_id", Types.UUIDType.get()),
                    Types.NestedField.required(
                            10_301, "table_reference", TableReference.ICEBERG_SCHEMA),
                    Types.NestedField.optional(
                            10_302,
                            "data_files",
                            Types.ListType.ofRequired(10_303, DataFile.getType(spec.partitionType()))),
                    Types.NestedField.optional(
                            10_304,
                            "delete_files",
                            Types.ListType.ofRequired(10_304, DataFile.getType(spec.partitionType())))));
  }

  @Test
  public void testCommitReadyBecomesDataComplete() {
    io.tabular.iceberg.connect.events.Event event =
        new io.tabular.iceberg.connect.events.Event(
            "cg-connector",
            EventType.COMMIT_READY,
            new CommitReadyPayload(
                commitId,
                Arrays.asList(
                    new TopicPartitionOffset("topic", 1, 1L, 1L),
                    new TopicPartitionOffset("topic", 2, null, null)),
                Arrays.asList(
                        new TopicPartitionTxId("topic", 1, 1L),
                        new TopicPartitionTxId("topic", 2, null))));


    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);
    assertThat(event.groupId()).isEqualTo("cg-connector");

    assertThat(result.type()).isEqualTo(PayloadType.DATA_COMPLETE);
    assertThat(result.payload()).isInstanceOf(TransactionDataComplete.class);
    TransactionDataComplete payload = (TransactionDataComplete) result.payload();

    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.assignments().get(0).topic()).isEqualTo("topic");
    assertThat(payload.assignments().get(0).partition()).isEqualTo(1);
    assertThat(payload.assignments().get(0).offset()).isEqualTo(1L);
    assertThat(payload.assignments().get(0).timestamp())
        .isEqualTo(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneOffset.UTC));

    assertThat(payload.assignments().get(1).topic()).isEqualTo("topic");
    assertThat(payload.assignments().get(1).partition()).isEqualTo(2);
    assertThat(payload.assignments().get(1).offset()).isNull();
    assertThat(payload.assignments().get(1).timestamp()).isNull();

    assertThat(payload.txIds().get(0).topic()).isEqualTo("topic");
    assertThat(payload.txIds().get(0).partition()).isEqualTo(1);
    assertThat(payload.txIds().get(0).txId()).isEqualTo(1L);

    assertThat(payload.txIds().get(1).topic()).isEqualTo("topic");
    assertThat(payload.txIds().get(1).partition()).isEqualTo(2);
    assertThat(payload.txIds().get(1).txId()).isNull();
  }

  @Test
  public void testCommitTableBecomesCommitToTable() {
    io.tabular.iceberg.connect.events.Event event =
        new io.tabular.iceberg.connect.events.Event(
            "cg-connector",
            EventType.COMMIT_TABLE,
            new CommitTablePayload(
                commitId, new TableName(Collections.singletonList("db"), "tbl"), 1L, 2L));

    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);
    assertThat(event.groupId()).isEqualTo("cg-connector");
    assertThat(result.type()).isEqualTo(PayloadType.COMMIT_TO_TABLE);
    assertThat(result.payload()).isInstanceOf(CommitToTable.class);
    CommitToTable payload = (CommitToTable) result.payload();

    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.snapshotId()).isEqualTo(1L);
    assertThat(payload.validThroughTs())
        .isEqualTo(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2L), ZoneOffset.UTC));
    assertThat(payload.tableReference().catalog()).isEqualTo(catalogName);
    assertThat(payload.tableReference().identifier()).isEqualTo(TableIdentifier.of("db", "tbl"));
  }

  @Test
  public void testCommitTableBecomesCommitToTableNullVtts() {
    io.tabular.iceberg.connect.events.Event event =
            new io.tabular.iceberg.connect.events.Event(
                    "cg-connector",
                    EventType.COMMIT_TABLE,
                    new CommitTablePayload(
                            commitId, new TableName(Collections.singletonList("db"), "tbl"), 1L, null));

    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);
    assertThat(event.groupId()).isEqualTo("cg-connector");
    assertThat(result.type()).isEqualTo(PayloadType.COMMIT_TO_TABLE);
    assertThat(result.payload()).isInstanceOf(CommitToTable.class);
    CommitToTable payload = (CommitToTable) result.payload();

    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.snapshotId()).isEqualTo(1L);
    assertThat(payload.validThroughTs()).isNull();
    assertThat(payload.tableReference().catalog()).isEqualTo(catalogName);
    assertThat(payload.tableReference().identifier()).isEqualTo(TableIdentifier.of("db", "tbl"));
  }

  @Test
  public void testCommitCompleteBecomesCommitCompleteSerialization() {
    io.tabular.iceberg.connect.events.Event event =
        new io.tabular.iceberg.connect.events.Event(
            "cg-connector", EventType.COMMIT_COMPLETE, new CommitCompletePayload(commitId, 2L));

    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);
    assertThat(result.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    assertThat(result.payload()).isInstanceOf(CommitComplete.class);
    CommitComplete payload = (CommitComplete) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.validThroughTs())
        .isEqualTo(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2L), ZoneOffset.UTC));
  }

  @Test
  public void testCommitCompleteBecomesCommitCompleteSerializationNullVtts() {
    io.tabular.iceberg.connect.events.Event event =
            new io.tabular.iceberg.connect.events.Event(
                    "cg-connector", EventType.COMMIT_COMPLETE, new CommitCompletePayload(commitId, null));

    byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);

    Event result = eventDecoder.decode(data);
    assertThat(result.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    assertThat(result.payload()).isInstanceOf(CommitComplete.class);
    CommitComplete payload = (CommitComplete) result.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.validThroughTs()).isNull();
  }
}
