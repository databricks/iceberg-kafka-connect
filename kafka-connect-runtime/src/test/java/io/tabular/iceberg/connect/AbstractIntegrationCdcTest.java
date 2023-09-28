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
import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
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

public abstract class AbstractIntegrationCdcTest extends IntegrationTestBase {

  private static final String TEST_DB = "test";
  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @BeforeEach
  public void setup() {
    createTopic(testTopic, TEST_TOPIC_PARTITIONS);
    ((SupportsNamespaces) catalog).createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void teardown() {
    context.stopKafkaConnector(connectorName);
    deleteTopic(testTopic);
    catalog.dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
    ((SupportsNamespaces) catalog).dropNamespace(Namespace.of(TEST_DB));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"test_branch"})
  public void testIcebergSinkPartitionedTable(String branch) {
    catalog.createTable(
        TABLE_IDENTIFIER, TEST_SCHEMA, TEST_SPEC, ImmutableMap.of(FORMAT_VERSION, "2"));

    runTest(branch);

    List<DataFile> files = getDataFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(2, 3);
    assertEquals(4, files.stream().mapToLong(DataFile::recordCount).sum());

    List<DeleteFile> deleteFiles = getDeleteFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(2, 3);
    assertEquals(2, deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum());

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"test_branch"})
  public void testIcebergSinkUnpartitionedTable(String branch) {
    catalog.createTable(TABLE_IDENTIFIER, TEST_SCHEMA, null, ImmutableMap.of(FORMAT_VERSION, "2"));

    runTest(branch);

    List<DataFile> files = getDataFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertEquals(4, files.stream().mapToLong(DataFile::recordCount).sum());

    List<DeleteFile> deleteFiles = getDeleteFiles(TABLE_IDENTIFIER, branch);
    // may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(1, 2);
    assertEquals(2, deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum());

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  private void runTest(String branch) {
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
            .config("value.converter.schemas.enable", false)
            .config("iceberg.tables", format("%s.%s", TEST_DB, TEST_TABLE))
            .config("iceberg.tables.cdcField", "op")
            .config("iceberg.control.commitIntervalMs", 1000)
            .config("iceberg.control.commitTimeoutMs", Integer.MAX_VALUE)
            .config("iceberg.kafka.auto.offset.reset", "earliest");
    connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.defaultCommitBranch", branch);
    }

    context.startKafkaConnector(connectorConfig);

    // start with 3 records, update 1, delete 1. Should be a total of 4 adds and 2 deletes
    // (the update will be 1 add and 1 delete)

    TestEvent event1 = new TestEvent(1, "type1", System.currentTimeMillis(), "hello world!", "I");
    TestEvent event2 = new TestEvent(2, "type2", System.currentTimeMillis(), "having fun?", "I");

    long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();
    TestEvent event3 = new TestEvent(3, "type3", threeDaysAgo, "hello from the past!", "I");

    TestEvent event4 = new TestEvent(1, "type1", System.currentTimeMillis(), "hello world!", "D");
    TestEvent event5 = new TestEvent(3, "type3", threeDaysAgo, "updated!", "U");

    send(testTopic, event1);
    send(testTopic, event2);
    send(testTopic, event3);
    send(testTopic, event4);
    send(testTopic, event5);
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
