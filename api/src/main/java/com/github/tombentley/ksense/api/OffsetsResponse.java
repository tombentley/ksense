/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tombentley.ksense.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OffsetsResponse {

    public static final String PATH = "/v1/offsets";

    public static class TopicOffsets {
        private final String topic;
        private final List<PartitionOffsets> partitionOffsets;

        @JsonCreator
        public TopicOffsets(
                @JsonProperty("topic") String topic,
                @JsonProperty("partitions") List<PartitionOffsets> partitionOffsets) {
            this.topic = topic;
            this.partitionOffsets = partitionOffsets;
        }

        @JsonProperty("topic")
        public String topic() {
            return topic;
        }

        @JsonProperty("partitions")
        public List<PartitionOffsets> partitionOffsets() {
            return partitionOffsets;
        }
    }

    public static class PartitionOffsets {
        private final int partitionId;
        private final long leo;

        @JsonCreator
        public PartitionOffsets(
                @JsonProperty("partition") int partitionId,
                @JsonProperty("leo") long leo) {
            this.partitionId = partitionId;
            this.leo = leo;
        }

        @JsonProperty("partition")
        public int partitionId() {
            return partitionId;
        }

        @JsonProperty("leo")
        public long leo() {
            return leo;
        }
    }

    private final long time;

    private final List<TopicOffsets> topicsOffsets;

    @JsonCreator
    public OffsetsResponse(
            @JsonProperty("time") long time,
            @JsonProperty("offsets") List<TopicOffsets> topicsOffsets) {
        this.time = time;
        this.topicsOffsets = topicsOffsets;
    }

    /**
     * @return The number of milliseconds since the UNIX epoch that the observations were taken.
     */
    @JsonProperty("time")
    public long time() {
        return time;
    }

    @JsonProperty("offsets")
    public List<TopicOffsets> offsets() {
        return topicsOffsets;
    }
}
