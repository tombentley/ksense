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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.github.tombentley.ksense.api.BrokerStateResponse;
import com.github.tombentley.ksense.api.ZkSessionStateResponse;
import com.github.tombentley.ksense.api.OffsetsResponse;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientImpl implements Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientImpl.class);

    private final Vertx vertx;
    private final Admin admin;
    private final Map<Integer, InetSocketAddress> cluster;
    private final WebClient client;

    public ClientImpl(Vertx vertx, Admin admin, Map<Integer, InetSocketAddress> cluster) {
        this.admin = admin;
        this.vertx = vertx;
        this.cluster = cluster;
        WebClientOptions options = new WebClientOptions()
                .setKeepAlive(false);
        client = WebClient.create(vertx, options);
    }


    private <T> Future<T> asFuture(String description, KafkaFuture<T> kafkaFuture) {
        Promise<T> promise = Promise.promise();
        LOGGER.trace("Promised future {}", description);
        kafkaFuture.toCompletionStage().whenComplete((r, e) -> {
            if (e != null) {
                LOGGER.trace("Admin client completed future {} erroneously", description, e);
            } else {
                LOGGER.trace("Admin client completed future {} successfully", description);
            }
            vertx.runOnContext(i -> {
                if (e == null) {
                    LOGGER.trace("Vertx completed future {} successfully", description);
                    promise.complete(r);
                } else {
                    LOGGER.trace("Vertx completed future {} erroneously", description, e);
                    promise.fail(e);
                }
            });
        });
        return promise.future();
    }

    Future<ClusterState> cluster() {
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        CompositeFuture cf = CompositeFuture.join(asFuture("cluster id", describeClusterResult.clusterId()),
                asFuture("controller", describeClusterResult.controller()),
                asFuture("live nodes", describeClusterResult.nodes()));

        return cf.map((CompositeFuture x) ->
                new ClusterState(x.resultAt(0),
                        ((Node) x.resultAt(1)).id(),
                        ((Collection<Node>) x.resultAt(2)).stream()
                                .map(Node::id)
                                .collect(Collectors.toUnmodifiableSet())));
    }

    Future<Set<String>> listTopics(boolean includeInternal) {
        return asFuture("list topics", admin.listTopics(new ListTopicsOptions().listInternal(includeInternal)).names());
    }

    Future<Map<String, TopicDescription>> describeTopics(Set<String> topicNames) {
        return asFuture("describe topics", admin.describeTopics(topicNames).all());
    }

    Future<Map<String, Map<String, ConfigEntry>>> describeTopicConfigs(Set<String> topicNames) {
        Set<ConfigResource> topicResources = topicNames.stream()
                .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toSet());
        return asFuture("describe topic configs", admin.describeConfigs(topicResources).all()).map(this::topicConfigsAsMap);
    }

    private Map<String, Map<String, ConfigEntry>> topicConfigsAsMap(Map<ConfigResource, Config> tc) {
        return tc.entrySet().stream()
                .collect(Collectors.<Map.Entry<ConfigResource, Config>, String, Map<String, ConfigEntry>>toMap(
                        entry -> entry.getKey().name(),
                        entry -> entry.getValue().entries().stream()
                                .collect(Collectors.<ConfigEntry, String, ConfigEntry>toMap(
                                        ConfigEntry::name,
                                        configEntry -> configEntry))));
    }

    Future<BrokerStateResponse.State> requestBrokerState(int brokerId) {
        var addr = cluster.get(brokerId);
        return client.get(addr.getPort(), addr.getHostString(), BrokerStateResponse.PATH)
                .as(BodyCodec.json(BrokerStateResponse.class))
                .send()
                .map(HttpResponse::body)
                .map(bs -> BrokerStateResponse.State.fromBrokerState(bs.code()));
    }

    Future<ZkSessionStateResponse.State> requestZkSessionState(int brokerId) {
        var addr = cluster.get(brokerId);
        return client.get(addr.getPort(), addr.getHostString(), ZkSessionStateResponse.PATH)
                .as(BodyCodec.json(ZkSessionStateResponse.class))
                .send()
                .map(HttpResponse::body)
                .map(sessionState -> ZkSessionStateResponse.State.fromState(sessionState.description()));
    }

    Future<OffsetsResponse> requestOffsets(int brokerId) {
        var addr = cluster.get(brokerId);
        return client.get(addr.getPort(), addr.getHostString(), OffsetsResponse.PATH)
                .as(BodyCodec.json(OffsetsResponse.class))
                .send()
                .map(HttpResponse::body);
    }

    @Override
    public Future<BrokerState> brokerState(int brokerId) {
        var clusterFuture = cluster();
        var partitionsOnBrokerFuture = listTopics(true)
                .compose(this::describeTopics)
                .compose(topicDescriptionMap -> partitionStatesForBroker(brokerId, topicDescriptionMap));
        var brokerStateFuture = requestBrokerState(brokerId);
        var zkStateFuture = requestZkSessionState(brokerId);
        return CompositeFuture.join(clusterFuture, partitionsOnBrokerFuture, brokerStateFuture, zkStateFuture).map(cf -> {
            ClusterState clusterState = cf.resultAt(0);
            List<PartitionState> partitionStates = cf.resultAt(1);
            BrokerStateResponse.State brokerState = cf.resultAt(2);
            ZkSessionStateResponse.State zkState = cf.resultAt(3);
            return new BrokerState(brokerId, brokerState, zkState, clusterState, partitionStates);
        });
    }



    /** Get the log end offsets for all logs held by the given broker */
    @Override
    public Future<LogEndOffsets> logEndOffsets(int brokerId) {
        return requestOffsets(brokerId).map(o -> {
            Map<TopicPartition, Long> map = new HashMap<>();
            for (OffsetsResponse.TopicOffsets t : o.offsets()) {
                for (OffsetsResponse.PartitionOffsets p : t.partitionOffsets()) {
                    TopicPartition topicPartition = new TopicPartition(t.topic(), p.partitionId());
                    map.put(topicPartition, p.leo());
                }
            }
            return new LogEndOffsets(o.time(), map);
        });
    }

    private Future<List<PartitionState>> partitionStatesForBroker(int brokerId, Map<String, TopicDescription> topics) {
        // Find the partitions replicated by broker
        var replicatedPartitions = topics.values().stream()
                .flatMap(PartitionState::fromTopic)
                .filter(ps -> ps.replicas().contains(brokerId)).collect(Collectors.toList());

        // Get the topics for those partitions
        var replicatedTopics = replicatedPartitions.stream().map(PartitionState::topicName).collect(Collectors.toSet());

        return describeTopicConfigs(replicatedTopics).map(map -> replicatedPartitions.stream()
                // Augment with min ISR
                .map(ps -> ps.withMinIsr(Integer.parseInt(map.get(ps.topicName()).get("min.insync.replicas").value())))
                // sort
                .sorted(Comparator.comparing(PartitionState::topicName)
                        .thenComparing(PartitionState::partitionId))
                .collect(Collectors.toList()));

    }

    @Override
    public void close() {
        client.close();
        vertx.close();
        admin.close();
    }
}
