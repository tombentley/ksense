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

/**
 * Enumerates possibilities for replication lag state: catching up, falling behind, etc.
 */
public abstract class ReplicationLagState { // oh, for a sealed class

    private ReplicationLagState() {
    }

    /** The follower is catching up with the leader */
    public static class CatchingUp extends ReplicationLagState {
        public final double predictedSeconds;

        public CatchingUp(double predictedSeconds) {
            this.predictedSeconds = predictedSeconds;
        }
    }

    /** The follower is fetching, but is dropping behind the leader */
    public static class DroppingBehind extends ReplicationLagState {
        @Override
        public String toString() {
            return "DroppingBehind";
        }
    }

    /** The follower is not fetching, and is dropping behind the leader */
    public static class NotReplicating extends ReplicationLagState {
        private final String description;

        public NotReplicating(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return "NotReplicating(" +
                    "description='" + description + '\'' +
                    ')';
        }
    }

    /** The situation is unclear within the required confidence */
    public static class Unclear extends ReplicationLagState {
        private final String description;

        public Unclear(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return "Unclear(" +
                    "description='" + description + '\'' +
                    ')';
        }
    }
}
