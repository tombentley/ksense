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
package com.github.tombentley.ksense.agent;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import kafka.metrics.KafkaYammerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static JVM agent that gets a RouterHolder instance and adds a {@link StatusEndpoints} that's
 * listening to the {@code KafkaYammerMetrics} default registry.
 */
public class Agent {

    private static final Logger LOGGER = LoggerFactory.getLogger(Agent.class);

    public static void premain(String agentArgs) throws IOException {
        LOGGER.info("Entered {}#premain()", Agent.class.getName());
        Map<String, String> config;
        if (!agentArgs.isEmpty()) {
            var configFile = new File(agentArgs);
            config = loadConfig(configFile);
        } else {
            config = Map.of();
        }
        var routerHolder = RouterHolder.instance(config);
        KafkaYammerMetrics.defaultRegistry().addListener(new StatusEndpoints(routerHolder.router(), routerHolder::tcpConnect));
        KafkaYammerMetrics.defaultRegistry().addListener(new OffsetsEndpoint(routerHolder.router()));
        LOGGER.info("Returning from {}#premain()", Agent.class.getName());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, String> loadConfig(File configFile) throws IOException {
        Properties config = new Properties();
        try (var reader = new FileReader(configFile)) {
            config.load(reader);
        }
        return (Map) config;
    }
}
