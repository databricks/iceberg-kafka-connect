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

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerFactoryImpl implements ProducerFactory {
  @Override
  public Pair<UUID, Producer<String, byte[]>> create(Map<String, String> props) {
    UUID producerId = UUID.randomUUID();

    Map<String, Object> producerProps = Maps.newHashMap(props);
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    KafkaProducer<String, byte[]> producer =
        new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());

    return Pair.of(producerId, producer);
  }
}
