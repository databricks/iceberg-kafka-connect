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
package io.tabular.iceberg.connect.events;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

import java.util.List;

public class TopicPartitionTransaction implements IndexedRecord {

    private String topic;
    private Integer partition;
    private Long txId;
    private final Schema avroSchema;

    static final int TOPIC = 10_800;
    static final int PARTITION = 10_801;
    static final int TX_ID = 10_802;

    public static final Types.StructType ICEBERG_SCHEMA =
            Types.StructType.of(
                    Types.NestedField.required(TOPIC, "topic", Types.StringType.get()),
                    Types.NestedField.required(PARTITION, "partition", Types.IntegerType.get()),
                    Types.NestedField.optional(TX_ID, "txId", Types.LongType.get()));

    private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(ICEBERG_SCHEMA, TopicPartitionTransaction.class.getName());

    public TopicPartitionTransaction(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public TopicPartitionTransaction(String topic, Integer partition, Long txId) {
        this.topic = topic;
        this.partition = partition;
        this.txId = txId;
        this.avroSchema = AVRO_SCHEMA;
    }

    public String topic() {
        return topic;
    }

    public Integer partition() {
        return partition;
    }

    public Long txId() {
        return txId;
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public void put(int i, Object v) {
        switch (positionToId(i, avroSchema)) {
            case TOPIC:
                this.topic = v == null ? null : v.toString();
                break;
            case PARTITION:
                this.partition = (Integer) v;
                break;
            case TX_ID:
                this.txId = (Long) v;
                break;
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }
    
    @Override
    public Object get(int i) {
        switch (positionToId(i, avroSchema)) {
            case TOPIC:
                return topic;
            case PARTITION:
                return partition;
            case TX_ID:
                return txId;
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }

    static int positionToId(int position, Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        Preconditions.checkArgument(
                position >= 0 && position < fields.size(), "Invalid field position: " + position);
        Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
        return val == null ? -1 : (int) val;
    }
}
