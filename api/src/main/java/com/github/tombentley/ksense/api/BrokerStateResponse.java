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

public class BrokerStateResponse {

    public static final String PATH = "/v1/broker-state";

    private final int code;

    @JsonCreator
    public BrokerStateResponse(@JsonProperty("code") int code) {
        this.code = code;
    }

    @JsonProperty("code")
    public int code() {
        return code;
    }

    @Override
    public String toString() {
        return "BrokerStateResponse(" +
                "code=" + code + ')';
    }

    public enum State {
        NOT_RUNNING(true, false, false),
        STARTING(true, false, false),
        RECOVERY(true, false, false),
        RUNNING(false, true, false),
        PENDING_CONTROLLED_SHUTDOWN(false, false, true),
        SHUTTING_DOWN(false, false, true),
        UNKNOWN(false, false, true);

        public boolean startingUp;
        public boolean running;
        public boolean shuttingDown;

        State(boolean startingUp, boolean running, boolean shuttingDown) {

        }

        public static State fromBrokerState(int state) {
            switch (state) {
                case 0: //
                    return NOT_RUNNING;
                case 1: // (not yet caught up with metadata)
                    return STARTING;
                case 2: // (still fenced)
                    return RECOVERY;
                case 3: // (accepting client requests)
                    return RUNNING;
                case 6: // PENDING_CONTROLLED_SHUTDOWN
                    return PENDING_CONTROLLED_SHUTDOWN;
                case 7: // SHUTTING_DOWN
                    return SHUTTING_DOWN;
                default:
                    return UNKNOWN;
            }
        }
    }
}
