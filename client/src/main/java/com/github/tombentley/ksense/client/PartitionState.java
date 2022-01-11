package com.github.tombentley.ksense.client;/*
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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;

public class PartitionState {
    private final String topicName;
    private final int minIsr;
    private final int partitionId;
    private final Set<Integer> isr;
    private final Set<Integer> replicas;
    private final int leader;

    private PartitionState(String topicName, int partitionId, int leader, Set<Integer> replicas, int minIsr, Set<Integer> isr) {
        this.topicName = topicName;
        this.minIsr = minIsr;
        this.partitionId = partitionId;
        this.isr = isr;
        this.replicas = replicas;
        this.leader = leader;
    }

    static Stream<PartitionState> fromTopic(TopicDescription td) {
        return td.partitions().stream().map(tpi ->
                new PartitionState(
                        td.name(),
                        tpi.partition(),
                        tpi.leader() == null ? -1 : tpi.leader().id(),
                        tpi.replicas().stream().map(Node::id).collect(Collectors.toSet()),
                        -1,
                        tpi.isr().stream().map(Node::id).collect(Collectors.toSet())
                ));
    }

    PartitionState withMinIsr(int minIsr) {
        return new PartitionState(this.topicName(), this.partitionId(), this.leader(), this.replicas(), minIsr, this.isr());
    }

    /** The name of the topic */
    public String topicName() {
        return topicName;
    }

    /** The index of the partition */
    public int partitionId() {
        return partitionId;
    }

    /** The ISR of the partition */
    public Set<Integer> isr() {
        return isr;
    }

    /** The out-of-sync replicas of the partition: <code>{@link #replicas()} - {@link #isr()}</code>. */
    public Set<Integer> oosr() {
        var outOfSync = new HashSet<>(replicas());
        outOfSync.removeAll(isr());
        return outOfSync;
    }

    /** The brokers replicating the partition */
    public Set<Integer> replicas() {
        return replicas;
    }

    /** The leader of the partition, or -1 if there is no curreny leader */
    public int leader() {
        return leader;
    }

    /**
     * The min.insync.replicas for this partition's topic, or -1 if this is not known.
     * @return
     */
    public int minIsr() {
        return minIsr;
    }

    @Override
    public String toString() {
        return "PartitionState(" +
                "topicName='" + topicName + '\'' +
                ", partitionId=" + partitionId +
                ", replicas=" + replicas +
                ", leader=" + leader +
                ", isr=" + isr +
                ", minIsr=" + minIsr +
                ')';
    }
}
