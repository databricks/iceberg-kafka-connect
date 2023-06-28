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

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.events.Event;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

  private final String controlTopic;
  private final String controlGroupId;
  private final String groupId;
  private final Producer<String, byte[]> producer;
  private final Consumer<String, byte[]> consumer;
  private final Admin admin;
  private final Map<Integer, Long> controlTopicOffsets = new HashMap<>();
  private final String producerId;

  public Channel(
      String name,
      String consumerGroupId,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory) {
    this.controlTopic = config.getControlTopic();
    this.controlGroupId = config.getControlGroupId();
    this.groupId = config.getControlGroupId();

    String transactionalId = name + config.getTransactionalSuffix();
    this.producer = clientFactory.createProducer(transactionalId);
    this.consumer = clientFactory.createConsumer(consumerGroupId);
    this.admin = clientFactory.createAdmin();

    this.producerId = UUID.randomUUID().toString();
  }

  protected void send(Event event) {
    send(ImmutableList.of(event), ImmutableMap.of());
  }

  protected void send(List<Event> events, Map<TopicPartition, Offset> sourceOffsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    sourceOffsets.forEach((k, v) -> offsetsToCommit.put(k, new OffsetAndMetadata(v.getOffset())));

    List<ProducerRecord<String, byte[]>> recordList =
        events.stream()
            .map(
                event -> {
                  LOG.info("Sending event of type: {}", event.getType().name());
                  byte[] data = Event.encode(event);
                  // key by producer ID to keep event order
                  return new ProducerRecord<>(controlTopic, producerId, data);
                })
            .collect(toList());

    synchronized (producer) {
      producer.beginTransaction();
      try {
        recordList.forEach(producer::send);
        if (!sourceOffsets.isEmpty()) {
          // TODO: this doesn't fence zombies
          producer.sendOffsetsToTransaction(
              offsetsToCommit, new ConsumerGroupMetadata(controlGroupId));
        }
        producer.commitTransaction();
      } catch (Exception e) {
        try {
          producer.abortTransaction();
        } catch (Exception ex) {
          LOG.warn("Error aborting producer transaction", ex);
        }
        throw e;
      }
    }
  }

  protected abstract boolean receive(Envelope envelope);

  protected void consumeAvailable(Duration pollDuration) {
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      records.forEach(
          record -> {
            // the consumer stores the offsets that corresponds to the next record to consume,
            // so increment the record offset by one
            controlTopicOffsets.put(record.partition(), record.offset() + 1);

            Event event = Event.decode(record.value());

            if (event.getGroupId() == null || event.getGroupId().equals(groupId)) {
              LOG.debug("Received event of type: {}", event.getType().name());
              if (receive(new Envelope(event, record.partition(), record.offset()))) {
                LOG.info("Handled event of type: {}", event.getType().name());
              }
            }
          });
      records = consumer.poll(pollDuration);
    }
  }

  protected Map<Integer, Long> getControlTopicOffsets() {
    return controlTopicOffsets;
  }

  protected void commitConsumerOffsets() {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    getControlTopicOffsets()
        .forEach(
            (k, v) ->
                offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
    consumer.commitSync(offsetsToCommit);
  }

  protected Admin admin() {
    return admin;
  }

  public void start() {
    consumer.subscribe(ImmutableList.of(controlTopic));

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofMillis(1000));
  }

  public void stop() {
    LOG.info("Channel stopping");
    producer.close();
    consumer.close();
    admin.close();
  }
}
