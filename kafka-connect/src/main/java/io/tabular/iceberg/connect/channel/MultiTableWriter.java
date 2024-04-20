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

import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriterResult;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.sink.SinkRecord;

class MultiTableWriter implements TableWriter, AutoCloseable {

  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;

  MultiTableWriter(IcebergWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }

  @Override
  public void write(SinkRecord sinkRecord, String tableName, boolean ignoreMissingTable) {
    writerForTable(tableName, sinkRecord, ignoreMissingTable).write(sinkRecord);
  }

  private void closeWriters() {
    writers.values().forEach(Utilities::close);
    writers.clear();
  }

  @Override
  public List<WriterResult> committable() {
    List<WriterResult> writerResults =
        writers.values().stream()
            .flatMap(writer -> writer.complete().stream())
            .collect(Collectors.toList());

    closeWriters();

    return writerResults;
  }

  @Override
  public void close() {
    closeWriters();
    Utilities.close(writerFactory);
  }
}
