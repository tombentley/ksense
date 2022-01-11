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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.tombentley.ksense.api.OffsetsResponse;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistryListener;
import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import org.apache.kafka.common.TopicPartition;

public class OffsetsEndpoint implements MetricsRegistryListener {

    private static final long T0 = System.currentTimeMillis();
    private static final long R0 = System.nanoTime();
    // Scope looks like this: partition.0.topic.foo
    // TODO is there ans escaping on dots in the topic name?
    private static final Pattern P = Pattern.compile("partition\\.([0-9]+)");
    private static final Pattern T = Pattern.compile("topic\\.([a-zA-Z0-9_.-]+)");
    private final Map<TopicPartition, Gauge<? extends Number>> gauges = new ConcurrentHashMap<>();

    public OffsetsEndpoint(Router router) {
        router.get(OffsetsResponse.PATH)
                //.consumes("application/json")
                //.produces(OffsetsResponse.CONTENT_TYPE)
                .respond(rc -> {
                    Map<String, OffsetsResponse.TopicOffsets> topicOffsets = new HashMap<>();
                    gauges.forEach((tp, g) -> {
                        var to = topicOffsets.computeIfAbsent(tp.topic(), k -> new OffsetsResponse.TopicOffsets(k, new ArrayList<>()));
                        Number value = g.value();
                        if (value != null) {
                            to.partitionOffsets().add(new OffsetsResponse.PartitionOffsets(tp.partition(), value.longValue()));
                        }
                    });
                    return Future.succeededFuture(new OffsetsResponse(now(),
                            new ArrayList<>(topicOffsets.values())));
                });
    }

    private long now() {
        return T0 + ((System.nanoTime() - R0) / 1_000_000);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        if (isOffsetMetric(name) && metric instanceof Gauge<?>) {
            gauges.put(tp(name), (Gauge<? extends Number>) metric);
        }
    }

    private boolean isOffsetMetric(MetricName name) {
        return "LogEndOffset".equals(name.getName())
                && "kafka.log".equals(name.getGroup())
                && "Log".equals(name.getType());
    }

    private static String topic(MetricName name) {
        Matcher matcher = T.matcher(name.getScope());
        if (!matcher.find()) {
            throw new IllegalArgumentException(name.getScope());
        }
        return matcher.group(1);
    }

    private static int partition(MetricName name) {
        Matcher matcher = P.matcher(name.getScope());
        if (!matcher.find()) {
            throw new IllegalArgumentException(name.getScope());
        }
        return Integer.parseInt(matcher.group(1));
    }

    private static TopicPartition tp(MetricName name) {
        return new TopicPartition(topic(name), partition(name));
    }

    @Override
    public void onMetricRemoved(MetricName name) {
        if (isOffsetMetric(name)) {
            gauges.remove(tp(name));
        }
    }
}
