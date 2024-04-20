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

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public interface WriteExceptionHandler {
  void initialize(SinkTaskContext context, IcebergSinkConfig config);

  class Result {
    private final SinkRecord sinkRecord;
    private final String tableName;

    public Result(SinkRecord sinkRecord, String tableName) {
      this.sinkRecord = sinkRecord;
      this.tableName = tableName;
    }

    public SinkRecord sinkRecord() {
      return sinkRecord;
    }

    public String tableName() {
      return tableName;
    }
  }

  /**
   * This method will be invoked whenever the connector runs into an exception while trying to write
   * SinkRecords to a table. Implementations of this method have 3 general options:
   *
   * <ol>
   *   <li>Return a SinkRecord and the name of the table to write to (wrapped inside a {@link
   *       Result})
   *   <li>Return null to drop the SinkRecord
   *   <li>Throw an exception to fail the task
   * </ol>
   *
   * @param sinkRecord The SinkRecord that couldn't be written
   * @param tableName The table the SinkRecord couldn't be written to
   * @param exception The exception encountered while trying to write the SinkRecord
   */
  Result handle(SinkRecord sinkRecord, String tableName, Exception exception) throws Exception;

  /**
   * This method will be invoked prior to committing allowing advanced {@link WriteExceptionHandler}
   * implementations to complete any inflight work before the connector commits.
   *
   * <p>Note that there is no guarantee that the connector will successfully commit after this
   * method is called.
   */
  void preCommit();
}
