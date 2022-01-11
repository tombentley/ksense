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
package com.github.tombentley.ksense.cli;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.github.tombentley.ksense.api.BrokerStateResponse;
import com.github.tombentley.ksense.api.ZkSessionStateResponse;
import com.github.tombentley.ksense.client.BrokerState;
import com.github.tombentley.ksense.client.Client;
import com.github.tombentley.ksense.client.LogEndOffsets;
import com.github.tombentley.ksense.client.PartitionState;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private final Client client;
    private final Vertx vertx;

    public static void main(String[] a) throws Exception {
        Properties config = new Properties();
//        try (var reader = new FileReader(a[1])) {
//            p.load(reader);
//        }
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //p.setProperty(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "5000");
        config.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        //p.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        int brokerId = Integer.parseInt(a[0]);
        Vertx vertx = Vertx.vertx();
        try (var client = Client.newInstance(vertx, (Map) config)) {
            Main main = new Main(vertx, client);
            //main.restartConsequences(brokerId);
            main.catchup(brokerId);
        }
    }

    public Main(Vertx vertx, Client client) {
        this.vertx = vertx;
        this.client = client;
    }

    private void restartConsequences(int brokerId) throws InterruptedException, ExecutionException {
        CompletableFuture<BrokerState> f = new CompletableFuture<>();
        // TODO possibly loop with timeout for inconsistent state (e.g. zookeeper sesssion != cluster metadata)
        client.brokerState(brokerId).onComplete(ar -> {
            if (ar.succeeded()) {
                f.complete(ar.result());
            } else {
                f.completeExceptionally(ar.cause());
            }
        });
        var state = f.get();
        logClusterState(state);
        logControllerChange(state);
        logLeaderChanges(state);
    }



    private void catchup(int brokerId) throws ExecutionException, InterruptedException {
        int numSamples = 5;
        int maxFetchLagMs = 30_000;
        var catchups = new Catchups(numSamples);
        CompletableFuture<BrokerState> f = new CompletableFuture<>();
        client.brokerState(brokerId).map(state -> {
            catchup(state, catchups).onComplete(new Handler<>() {
                int num = 1;
                @Override
                public void handle(AsyncResult<Catchups> event) {
                    if (event.succeeded()) {
                        LOGGER.info("{}", event.result().lags(brokerId + 1));
                        // increase the delay until it's 1/numSamples of the max lag
                        double targetMs = ((double) maxFetchLagMs) / numSamples;
                        double delayMs = ((double) Math.min(num, numSamples) / numSamples) * targetMs;
                        num++;
                        LOGGER.debug("Delay {}ms for next update", delayMs);
                        // Add 1ms to avoid a 0ms wait (which is an error).
                        vertx.setTimer((long) (1 + delayMs), timerId -> {
                            catchup(state, catchups).onComplete(this);
                        });
                    } else {
                        event.cause().printStackTrace();
                    }
                }
            });
            return null;
        });
        f.get();
    }

    private static <K, V> Future<Map<K, AsyncResult<V>>> mapJoin(Map<K, Future<V>> m) {
        var p = Promise.<Map<K, AsyncResult<V>>>promise();
        CompositeFuture.join(List.copyOf(m.values())).onComplete(cf -> {
            p.complete(m.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue)));
        });
        return p.future();
    }

    /** Get the log end offsets for all logs held by the given brokers */
    private Future<Map<Integer, AsyncResult<LogEndOffsets>>> leos(Set<Integer> brokerIds) {
        return mapJoin(brokerIds.stream().collect(
                Collectors.toMap(Function.identity(), client::logEndOffsets)));
    }

    public Future<Catchups> catchup(BrokerState brokerState,
                                    Catchups catchups) {
        Map<TopicPartition, PartitionState> partitionStates = partitionsStatesMap(brokerState.partitionStates());
        return Future.succeededFuture(partitionStates)
                .map(partitionStateMap -> {
                    Set<Integer> brokerIds = new HashSet<>();
                    for (var partitionState : partitionStateMap.values()) {
                        var outOfSync = partitionState.oosr();
                        // TODO sort out which we're interested in and why
                        brokerIds.add(partitionState.leader());
                        brokerIds.addAll(partitionState.replicas());
                    }
                    return brokerIds;
                })
                // TODO separate the getting of the states from the querying of the leos
                // how does the one cause the other to be updated?
                .compose(brokerIds -> {
                    LOGGER.debug("Requesting LEOs for {}", brokerIds);
                    return leos(brokerIds).map(leosByBroker -> {
                        LOGGER.debug("Response for LEOs for {}: {}", brokerIds, leosByBroker);
                        for (var partitionState : partitionStates.values()) {
                            var outOfSync = partitionState.oosr();
                            // TODO we only care about when brokerId is in outOfSync: We don't care about all brokers
                            //if (!outOfSync.isEmpty()) {
                                TopicPartition tp = tp(partitionState);
                                int leader = partitionState.leader();
                                if (leader == brokerState.brokerId()) {
                                    continue;
                                }
                                var leaderLeos = leosByBroker.get(leader);
                                catchups.leader(tp, leader, leaderLeos);
                                var followerLeos = leosByBroker.get(brokerState.brokerId());
                                catchups.follower(tp, brokerState.brokerId(), followerLeos);
                            //}
                        }
                        return catchups;
                    });
                });
    }

    private static TopicPartition tp(PartitionState partitionState) {
        return new TopicPartition(partitionState.topicName(), partitionState.partitionId());
    }

    private static Map<TopicPartition, PartitionState> partitionsStatesMap(List<PartitionState> partitionStateList) {
        Map<TopicPartition, PartitionState> result = new HashMap<>();
        for (var ps : partitionStateList) {
            result.put(tp(ps), ps);
        }
        return result;
    }

    private static void logClusterState(BrokerState state) {
        if (!state.isInCluster()) {
            switch (state.brokerState()) {
                case STARTING:
                    switch (state.zkSessionState()) {
                        case CONNECTED:
                            LOGGER.warn("Broker {} is not in the cluster metadata, but it has a connected zookeeper session and is starting: " +
                                    "Restarting will likely only delay recovery.",
                                    state.brokerId());
                            break;
                        case AUTH_FAILED:
                        case CONNECTEDREADONLY:
                            LOGGER.info("Broker {} is not in the cluster metadata and has zookeeper session state {}: " +
                                    "Something weird is going on",
                                    state.brokerId(), state.zkSessionState());
                            break;
                        case NOT_CONNECTED:
                        case CONNECTING:
                            LOGGER.info("Broker {} is not in the cluster metadata and has zookeeper session state {}: " +
                                    "Check ZK quorum exists; Waiting may help; Else restarting this broker may help",
                                    state.brokerId(), state.zkSessionState());
                            break;
                    }
                    break;
                case RUNNING:
                    // Cluster metadata disagrees with broker's zk session state
                    switch (state.zkSessionState()) {
                        case NOT_CONNECTED:
                        case CONNECTING:
                            LOGGER.info("Broker {} is running, but is {} to ZooKeeper: " +
                                    "Restarting may help, but so might patience.",
                                    state.brokerId(), state.zkSessionState());
                            break;
                        case AUTH_FAILED:
                            LOGGER.info("Broker {} cannot authenticate with ZooKeeper: " +
                                    "Restarting may help, but could be a configuration problem",
                                    state.brokerId());
                            break;
                        case CONNECTED:
                            LOGGER.info("Broker {} is connected to ZooKeeper, but not in the cluster metadata.",
                                    state.brokerId());
                            break;
                        case CONNECTEDREADONLY:
                            LOGGER.info("Broker {} is connected to ZooKeeper READONLY, which should never happen!",
                                    state.brokerId());
                            break;
                        default:
                            LOGGER.info("Broker {} has ZooKeeper session state {}.",
                                    state.brokerId(), state.zkSessionState());
                            break;
                    }
                    break;
                case SHUTTING_DOWN:
                    LOGGER.warn("Broker {} is not in the cluster but is shutting down: " +
                            "Restarting will likely only delay recovery",
                            state.brokerId());
                    break;
                default:
                    LOGGER.info("Broker {} is not in the cluster but is shutting down: " +
                            "Restarting will likely only delay recovery",
                            state.brokerId());
                    break;
            }
        } else {
            if (state.brokerState() == BrokerStateResponse.State.RUNNING && state.zkSessionState() == ZkSessionStateResponse.State.CONNECTED) {
                LOGGER.info("Broker {} is in the cluster, has broker state {} and ZK session state {}",
                        state.brokerId(),
                        state.brokerState(),
                        state.zkSessionState());
            } else {
                LOGGER.warn("Broker {} is in the cluster, has broker state {} and ZK session state {}",
                        state.brokerId(),
                        state.brokerState(),
                        state.zkSessionState());
            }
        }
    }

    private static void logControllerChange(BrokerState state) {
        if (state.isController()) {
            LOGGER.info("Broker {} is the controller: " +
                    "Restarting it will result in controller election (transparent to clients).",
                    state.brokerId());
        }
    }

    private static void logLeaderChanges(BrokerState state) {
        // The number of partitions this broker is leading.
        long numPartitionsLeading = state.partitionStates().stream().filter(p1 ->
                        p1.leader() == state.brokerId()).count();
        if (numPartitionsLeading > 0) {
            LOGGER.info("Broker {} is the leader of {} partitions: " +
                            "Restarting it will result in leader changes " +
                            "(transparent to clients, but will have observable latency spike).",
                    state.brokerId(), numPartitionsLeading);
        }

        final long numPartitionsAtMinIsr = state.atMinIsr().count();
        if (numPartitionsAtMinIsr > 0) {
            LOGGER.warn("Broker {} has {} in-sync partitions at min ISR: " +
                            "Restarting it will block acks=all producers.",
                    state.brokerId(), numPartitionsAtMinIsr);
            if (LOGGER.isDebugEnabled()) {
                state.atMinIsr().forEach(partitionState -> LOGGER.debug(" At min ISR: {}", partitionState));
            }
        }

        long numPartitionsBelowMinIsr = state.belowMinIsr().count();
        if (numPartitionsBelowMinIsr > 0) {
            LOGGER.info("Broker {} replicates {} below min ISR partitions: " +
                            "Restarting it may delay recovery.",
                    state.brokerId(), numPartitionsBelowMinIsr);
            if (LOGGER.isDebugEnabled()) {
                state.belowMinIsr().forEach(partitionState -> LOGGER.debug(" Below min ISR: {}", partitionState));
            }
        }

        long numPartitionsSingleIsr = state.onlyIsrMember().count();
        if (numPartitionsSingleIsr > 0) {
            LOGGER.warn("Broker {} is the only member of the ISR for {} partitions: " +
                        "Restarting it will block all producers and consumers (acks=all producers already affected).",
                    state.brokerId(), numPartitionsSingleIsr);
            if (LOGGER.isDebugEnabled()) {
                state.onlyIsrMember().forEach(partitionState -> LOGGER.debug(" Only ISR member: {}", partitionState));
            }
        }

        long numPartitionsEmptyIsr = state.emptyIsr().count();
        if (numPartitionsEmptyIsr > 0) {
            LOGGER.warn("Broker {} replicates {} below empty ISR partitions.",
                    state.brokerId(), numPartitionsEmptyIsr);
            if (LOGGER.isDebugEnabled()) {
                state.emptyIsr().forEach(partitionState -> LOGGER.debug(" Empty ISR: {}", partitionState));
            }
        }
    }
}
