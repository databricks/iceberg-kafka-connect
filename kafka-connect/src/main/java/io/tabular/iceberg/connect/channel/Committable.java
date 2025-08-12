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

import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.WriterResult;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.TopicPartition;
import java.util.stream.Collectors;

class Committable {

  private final ImmutableMap<TopicPartition, Offset> offsetsByTopicPartition;
  private final ImmutableMap<TopicPartition, Long> txIdsByTopicPartition;
  private final ImmutableList<WriterResult> writerResults;

  Committable(
          Map<TopicPartition, Offset> offsetsByTopicPartition, Map<TopicPartition, Long> txIdsByTopicPartition, List<WriterResult> writerResults) {
    this.offsetsByTopicPartition = ImmutableMap.copyOf(offsetsByTopicPartition);
    this.txIdsByTopicPartition = ImmutableMap.copyOf(
            txIdsByTopicPartition.entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );
    this.writerResults = ImmutableList.copyOf(writerResults);
  }

  public Map<TopicPartition, Offset> offsetsByTopicPartition() {
    return offsetsByTopicPartition;
  }

    public Map<TopicPartition, Long> txIdsByTopicPartition() {
        return txIdsByTopicPartition;
    }

  public List<WriterResult> writerResults() {
    return writerResults;
  }
}
