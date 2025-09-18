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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class KafkaUtilsTest {

    @Test
    public void testConsumerGroupMetadata() {
        String connectGroupId = "connect-abc";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9192");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, connectGroupId);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        WorkerSinkTaskContext context = new WorkerSinkTaskContext(kafkaConsumer, null, null);

        ConsumerGroupMetadata consumerGroupMetadata = KafkaUtils.consumerGroupMetadata(context);
        assertThat(consumerGroupMetadata).isEqualTo(kafkaConsumer.groupMetadata());
    }

    @Test
    public void testConsumerGroupMetadataThrowsErrorWhenNotGivenAWorkerSinkTaskContext() {
        SinkTaskContext context = mock(SinkTaskContext.class);
        assertThatThrownBy(() -> KafkaUtils.consumerGroupMetadata(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(String.format("Cannot bind field consumer to instance of class %s", context.getClass().getName()));
    }

}
