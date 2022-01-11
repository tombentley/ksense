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
import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;

public interface Client extends AutoCloseable {
    static Client newInstance(Vertx vertx, Map<String, Object> config) {
        return new ClientImpl(vertx, Admin.create(config),
                Map.of(0, InetSocketAddress.createUnresolved("localhost", 8080),
                        1, InetSocketAddress.createUnresolved("localhost", 8081),
                        2, InetSocketAddress.createUnresolved("localhost", 8082)));
    }

    /** Get the current state of the given broker */
    Future<BrokerState> brokerState(int brokerId);

    /** Get the log end offsets for all logs held by the given broker */
    Future<LogEndOffsets> logEndOffsets(int brokerId);

    void close();
}