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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ZkSessionStateResponse {

    public static final String PATH = "/v1/zk-session-state";

    private final String description;

    @JsonCreator
    public ZkSessionStateResponse(@JsonProperty("description") String description) {
        this.description = description;
    }

    @JsonProperty("description")
    public String description() {
        return description;
    }

    @Override
    public String toString() {
        return "DiscreteState(" +
                "description='" + description + '\'' +
                ')';
    }

    // Copied from org.apache.zookeeper.ZooKeeper.States
    public enum State {
        CONNECTING,
        ASSOCIATING,
        CONNECTED,
        CONNECTEDREADONLY,
        CLOSED,
        AUTH_FAILED,
        NOT_CONNECTED;

        public boolean isAlive() {
            return this != CLOSED && this != AUTH_FAILED;
        }

        /**
         * Returns whether we are connected to a server (which
         * could possibly be read-only, if this client is allowed
         * to go to read-only mode)
         * */
        public boolean isConnected() {
            return this == CONNECTED || this == CONNECTEDREADONLY;
        }

        public static State fromState(String state) {
            switch (state) {
                case "CONNECTING":
                    return CONNECTING;
                case "ASSOCIATING":
                    return ASSOCIATING;
                case "CONNECTED":
                    return CONNECTED;
                case "CONNECTEDREADONLY":
                    return CONNECTEDREADONLY;
                case "CLOSED":
                    return CLOSED;
                case "AUTH_FAILED":
                    return AUTH_FAILED;
                case "NOT_CONNECTED":
                    return NOT_CONNECTED;
            }
            throw new IllegalArgumentException(state);
        }
    }
}
