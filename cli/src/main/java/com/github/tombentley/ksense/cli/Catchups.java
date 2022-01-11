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
import java.util.Map;
import java.util.stream.Collectors;

import com.github.tombentley.ksense.client.LogEndOffsets;
import io.vertx.core.AsyncResult;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Catchups {
    private static final Logger LOGGER = LoggerFactory.getLogger(Catchups.class);

    /**
     * A catchup predication about a particular topic partition.
     */
    public static class Catchup {
        private final Pool<Integer, XYBuffer> offsetObservations;
        private int leader;

        public Catchup(int maxSamples) {
            this.offsetObservations = new Pool<>(() -> new XYBuffer(maxSamples));
            this.leader = -1;
        }

        // TODO time accounting: source process should report nominal epoch offset & nanos offsets from that
        // changes to epoch offset => restart
        // This process can use its own epoch offset to correlate times from all brokers
        // time here can be fractional seconds since that local epoch offset

        private static double toSeconds(long time) {
            return time / 1000.0;
        }

        public void recordOffset(int broker, long time, long offset) {
            // TODO if offset (or time) decreases then out imply truncation (e.g. due to leader change), so we should call reset()
            // so that historical (truncated) offsets don't bias the regression
            offsetObservations.get(broker).add(toSeconds(time), offset);
        }

        public void leader(int leader, long time, long offset) {
            //        if (this.leader != -1 && this.leader != leader) {
            //            // leader changed, presumably some truncation of un-replicated offsets
            //            // so we should discard some of the accumulated offsets
            //            this.leader = leader;
            //        }
            if (leader != -1) {
                this.leader = leader;
                recordOffset(leader, time, offset);
            }
        }

        public ReplicationLagState lagState(TopicPartition topicPartition, int brokerId) {
            if (brokerId == leader) {
                return null;
            }
            XYBuffer.BestFit leaderBestFit = offsetObservations.get(this.leader).leastSq();
            LOGGER.debug("{} leader ({}) best fit: {}", topicPartition, this.leader, leaderBestFit);
            XYBuffer.BestFit follower = offsetObservations.get(brokerId).leastSq();
            LOGGER.debug("{} follower ({}) best fit: {}", topicPartition, brokerId, follower);
            if (leaderBestFit.beta() < 1.0
                    && follower.beta() < 1E-3) {
                return new ReplicationLagState.NotReplicating("Follower fetch rate ≈ 0");
            } else {
                var tWorstCase = (leaderBestFit.alpha() + leaderBestFit.alphaDelta() - (follower.alpha() - follower.alphaDelta()))
                        /
                        (follower.beta() - follower.betaDelta() - (leaderBestFit.beta() + leaderBestFit.betaDelta()));
                var tBestCase = (leaderBestFit.alpha() - leaderBestFit.alphaDelta() - (follower.alpha() + follower.alphaDelta()))
                        /
                        (follower.beta() + follower.betaDelta() - (leaderBestFit.beta() - leaderBestFit.betaDelta()));
                var tCentral = (leaderBestFit.alpha() - follower.alpha())
                        /
                        (follower.beta() - leaderBestFit.beta());
                double now = toSeconds(System.currentTimeMillis());
                LOGGER.debug("{} time for {} to catch up with {} central={}s, best={}s, worst={}s (95% confidence)",
                        topicPartition,
                        brokerId, this.leader,
                        tCentral - now,
                        tBestCase - now,
                        tWorstCase - now);
                if (tWorstCase > now && tBestCase > now) {
                    return new ReplicationLagState.CatchingUp(tCentral - now);
                } else if (tWorstCase < now && tBestCase < now) {
                    return new ReplicationLagState.DroppingBehind();
                } else {
                    if (tCentral > now) {
                        return new ReplicationLagState.Unclear("Probably catching up, but not confident");
                    } else {
                        return new ReplicationLagState.Unclear("Probably falling behind, but not confident");
                    }
                }
            }
        }
    }

    private final int numSamples;
    private final Map<TopicPartition, Catchup> catchups = new HashMap<>();
    private final Map<TopicPartition, ReplicationLagState> lags = new HashMap<>();

    public Catchups(int numSamples) {
        this.numSamples = numSamples;
    }

    private Catchup get(TopicPartition topicPartition) {
        return catchups.computeIfAbsent(topicPartition, k -> new Catchup(numSamples));
    }

    public void leader(TopicPartition topicPartition, int leader, AsyncResult<LogEndOffsets> leos) {
        if (leos.succeeded()) {
            LogEndOffsets result = leos.result();
            long leo = result.leos().get(topicPartition);
            get(topicPartition).leader(leader, result.time(), leo);
        } else {
            lags.put(topicPartition, new ReplicationLagState.Unclear(leos.cause().toString()));
        }
    }

    public void follower(TopicPartition topicPartition, int follower, AsyncResult<LogEndOffsets> leos) {
        if (leos.succeeded()) {
            LogEndOffsets result = leos.result();
            long leo = result.leos().get(topicPartition);
            get(topicPartition).recordOffset(follower, result.time(), leo);
        } else {
            lags.put(topicPartition, new ReplicationLagState.Unclear(leos.cause().toString()));
        }
    }


    public ReplicationLagState lag(TopicPartition topicPartition, int followerId) {
        return get(topicPartition).lagState(topicPartition, followerId);
    }

    public Map<TopicPartition, ReplicationLagState> lags(int followerId) {
        return catchups.entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().lagState(entry.getKey(), followerId)));
    }

}
