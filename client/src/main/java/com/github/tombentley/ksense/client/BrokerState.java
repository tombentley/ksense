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

import java.util.List;
import java.util.stream.Stream;

import com.github.tombentley.ksense.api.BrokerStateResponse;
import com.github.tombentley.ksense.api.ZkSessionStateResponse;

public class BrokerState {

    private final BrokerStateResponse.State brokerState;
    private final ZkSessionStateResponse.State zkSessionState;
    private final ClusterState clusterState;
    private final List<PartitionState> partitionStates;
    private final int brokerId;

    public BrokerState(int brokerId,
                       BrokerStateResponse.State brokerState,
                       ZkSessionStateResponse.State zkSessionState,
                       ClusterState clusterState,
                       List<PartitionState> partitionStates) {
        this.brokerId = brokerId;
        this.brokerState = brokerState;
        this.zkSessionState = zkSessionState;
        this.clusterState = clusterState;
        this.partitionStates = partitionStates;
    }

    public int brokerId() {
        return brokerId;
    }

    public BrokerStateResponse.State brokerState() {
        return brokerState;
    }

    public ZkSessionStateResponse.State zkSessionState() {
        return zkSessionState;
    }

    public boolean isInCluster() {
        return clusterState().nodes().contains(brokerId());
    }

    public boolean isController() {
        return clusterState().controller() == brokerId();
    }

    public ClusterState clusterState() {
        return clusterState;
    }

    public List<PartitionState> partitionStates() {
        return partitionStates;
    }

    /**
     * The partitions where this broker is in the ISR and where {@code size(ISR) == minISR}.
     * I.e. if the broker drops out of the ISR (e.g. due to restart) then acks=all producers will be impacted.
     */
    public Stream<PartitionState> atMinIsr() {
        return partitionStates().stream().filter(partitionState ->
                partitionState.isr().contains(brokerId()) && partitionState.isr().size() == partitionState.minIsr());
    }

    /**
     * The partitions where this broker has a replica and where {@code size(ISR) < minISR}.
     * I.e. if the broker restarts then it can only further delay recovery of existing acks=all producers impacts.
     * either by making the ISR smaller or lengthening the time for the broker to catchup and rejoin the ISR
     */
    public Stream<PartitionState> belowMinIsr() {
        return partitionStates().stream().filter(partitionState ->
                partitionState.isr().size() < partitionState.minIsr());
    }

    /**
     * The partitions where this broker is the only broker in the ISR
     * I.e. if the broker drops out of the ISR (e.g. due to restart) then the partition will be offline and all producers and consumer will be affected
     */
    public Stream<PartitionState> onlyIsrMember() {
        return partitionStates().stream().filter(p1 ->
                p1.isr().contains(brokerId()) && p1.isr().size() == 1);
    }

    /**
     * The partitions where this broker has a replica and where {@code size(ISR) == 0}.
     * I.e. if the broker restarts then it can only further delay recovery of existing all producer and consumer impacts.
     * by lengthening the time for the broker to catchup and rejoin the ISR
     */
    public Stream<PartitionState> emptyIsr() {
        return partitionStates().stream().filter(p1 ->
                p1.isr().isEmpty());
    }

}
