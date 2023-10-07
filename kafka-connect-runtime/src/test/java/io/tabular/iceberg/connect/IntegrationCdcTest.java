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
package io.tabular.iceberg.connect;

import static io.tabular.iceberg.connect.TestEvent.TEST_SCHEMA;
import static io.tabular.iceberg.connect.TestEvent.TEST_SPEC;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class IntegrationCdcTest extends IntegrationTestBase {

  private static final String TEST_DB = "test";
  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @BeforeEach
  public void before() {
    createTopic(testTopic, TEST_TOPIC_PARTITIONS);
    ((SupportsNamespaces) catalog).createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void after() {
    context.stopConnector(connectorName);
    deleteTopic(testTopic);
    catalog.dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
    ((SupportsNamespaces) catalog).dropNamespace(Namespace.of(TEST_DB));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkPartitionedTable(String branch) {
    catalog.createTable(
        TABLE_IDENTIFIER, TEST_SCHEMA, TEST_SPEC, ImmutableMap.of(FORMAT_VERSION, "2"));

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema);

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(2, 3);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(4);

    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(2, 3);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(2);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSinkUnpartitionedTable(String branch) {
    catalog.createTable(TABLE_IDENTIFIER, TEST_SCHEMA, null, ImmutableMap.of(FORMAT_VERSION, "2"));

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema);

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(4);

    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(2);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  private void runTest(String branch, boolean useSchema) {
    // set offset reset to earliest so we don't miss any test messages
    KafkaConnectContainer.Config connectorConfig =
        new KafkaConnectContainer.Config(connectorName)
            .config("topics", testTopic)
            .config("connector.class", IcebergSinkConnector.class.getName())
            .config("tasks.max", 2)
            .config("consumer.override.auto.offset.reset", "earliest")
            .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("key.converter.schemas.enable", false)
            .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("value.converter.schemas.enable", useSchema)
            .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE))
            .config("iceberg.tables.cdc-field", "op")
            .config("iceberg.control.commit.interval-ms", 1000)
            .config("iceberg.control.commit.timeout-ms", Integer.MAX_VALUE)
            .config("iceberg.kafka.auto.offset.reset", "earliest");

    context.connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
    }

    if (!useSchema) {
      connectorConfig.config("value.converter.schemas.enable", false);
    }

    context.startConnector(connectorConfig);

    // start with 3 records, update 1, delete 1. Should be a total of 4 adds and 2 deletes
    // (the update will be 1 add and 1 delete)

    TestEvent event1 = new TestEvent(1, "type1", new Date(), "hello world!", "I");
    TestEvent event2 = new TestEvent(2, "type2", new Date(), "having fun?", "I");

    Date threeDaysAgo = Date.from(Instant.now().minus(Duration.ofDays(3)));
    TestEvent event3 = new TestEvent(3, "type3", threeDaysAgo, "hello from the past!", "I");

    TestEvent event4 = new TestEvent(1, "type1", new Date(), "hello world!", "D");
    TestEvent event5 = new TestEvent(3, "type3", threeDaysAgo, "updated!", "U");

    send(testTopic, event1, useSchema);
    send(testTopic, event2, useSchema);
    send(testTopic, event3, useSchema);
    send(testTopic, event4, useSchema);
    send(testTopic, event5, useSchema);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(this::assertSnapshotAdded);
  }

  private void assertSnapshotAdded() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    assertThat(table.snapshots()).hasSize(1);
  }
}
