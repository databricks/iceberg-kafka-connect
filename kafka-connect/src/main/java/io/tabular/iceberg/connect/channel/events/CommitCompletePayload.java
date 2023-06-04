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
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class CommitCompletePayload implements Payload {

  private UUID commitId;
  private Long vtts;
  private Schema avroSchema;

  public CommitCompletePayload(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public CommitCompletePayload(UUID commitId, Long vtts) {
    this.commitId = commitId;
    this.vtts = vtts;

    this.avroSchema =
        SchemaBuilder.builder()
            .record(getClass().getName())
            .fields()
            .name("commitId")
            .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
            .type(UUID_SCHEMA)
            .noDefault()
            .name("vtts")
            .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
            .type()
            .nullable()
            .longType()
            .noDefault()
            .endRecord();
  }

  public UUID getCommitId() {
    return commitId;
  }

  public Long getVtts() {
    return vtts;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.commitId = (UUID) v;
        return;
      case 1:
        this.vtts = (Long) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return commitId;
      case 1:
        return vtts;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
