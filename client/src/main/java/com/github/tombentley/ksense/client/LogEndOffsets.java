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
package com.github.tombentley.ksense.client;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * A collection of log end offsets observations on a particular broker at a particular time
 */
public class LogEndOffsets {
    private final long time;
    private final Map<TopicPartition, Long> leos;

    public LogEndOffsets(long time, Map<TopicPartition, Long> leos) {
        this.time = time;
        this.leos = leos;
    }

    public long time() {
        return time;
    }

    public Map<TopicPartition, Long> leos() {
        return leos;
    }

    @Override
    public String toString() {
        return "LogEndOffsets(" +
                "time=" + time +
                ", leos=" + leos +
                ')';
    }
}
