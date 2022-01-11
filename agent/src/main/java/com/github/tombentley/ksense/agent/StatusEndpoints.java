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

import java.util.function.BiFunction;

import com.github.tombentley.ksense.api.BrokerStateResponse;
import com.github.tombentley.ksense.api.ZkSessionStateResponse;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistryListener;
import io.vertx.core.Future;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Yammer {@code MetricsRegistryListener} that exposes REST endpoints:
 * <dl>
 *     <dt>{@code GET /state/broker}</dt>
 *     <dd>Reflects the BrokerState metric, returning the JSON representation of {@link ZkSessionStateResponse}</dd>
 *
 *     <dt>{@code GET /alive}</dt>
 *     <dd>Returns a 200 response:<ul>
 *         <li>When broker state is < RUNNING(3): the session state is CONNECTED</li>
 *         <li>When the broker state is >= RUNNING(3): a TCP connection can be made on localhost.</li>
 *     </ul>
 *     Otherwise returns 404.</dd>
 *
 *     <dt>{@code GET /ready}</dt>
 *     <dd>Returns a 200 response when the broker state is >= RUNNING(3) (thus a shutting down broker is considered ready).
 *     Otherwise returns 404.</dd>
 * </dl>
 */
class StatusEndpoints implements MetricsRegistryListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatusEndpoints.class);

    private static final String READY_PATH = "/v1/ready";
    private static final String ALIVE_PATH = "/v1/alive";
    private static final String TEXT_PLAIN_CONTENT_TYPE = "text/plain";

    private final Router router;
    private final BiFunction<String, Integer, Future<Void>> ping;
    private final int port;
    private final String host;

    // Access to the following requires synchronized
    private Route brokerStateRoute;
    private Route readinessRoute;
    private Gauge<? extends Number> brokerState;
    private Gauge<?> zkSessionState;
    private Route aliveRoute;
    private Route zkSessionRoute;

    public StatusEndpoints(Router router, BiFunction<String, Integer, Future<Void>> ping) {
        this.router = router;
        this.ping = ping;
        this.host = "localhost";
        this.port = 9092;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        if (isBrokerState(name) && metric instanceof Gauge) {
            synchronized (this) {
                this.brokerState = (Gauge<? extends Number>) metric;
                addBrokerStateRoute(this.brokerState);
                addReadyRoute(this.brokerState);
            }
        } else if (isZkSessionState(name) && metric instanceof Gauge) {
            synchronized (this) {
                this.zkSessionState = (Gauge<?>) metric;
                addZkSessionStateRoute(this.zkSessionState);
                addAliveRoute(this.zkSessionState);
            }
        }
    }

    @Override
    public void onMetricRemoved(MetricName name) {
        if (isBrokerState(name)) {
            synchronized (this) {
                removeBrokerStateRoute();
                removeReadyRoute();
                this.brokerState = null;
            }
        } else if (isZkSessionState(name)) {
            synchronized (this) {
                removeZkSessionStateRoute();
                removeAliveRoute();
                this.zkSessionState = null;
            }
        }
    }

    private boolean isBrokerState(MetricName name) {
        return "BrokerState".equals(name.getName())
                && "kafka.server".equals(name.getGroup())
                && "KafkaServer".equals(name.getType());
    }

    private synchronized void addBrokerStateRoute(Gauge<? extends Number> gauge) {
        if (brokerStateRoute == null) {
            LOGGER.info("Adding route for path={}", BrokerStateResponse.PATH);
            this.brokerStateRoute = router.get(BrokerStateResponse.PATH)
                    .respond(ctx -> {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Handling {} {}", ctx.request().method(), ctx.request().path());
                        }
                        int value = gauge.value().intValue();
                        return Future.succeededFuture(new BrokerStateResponse(value));
                    });
        }
    }

    private String brokerStateDescription(int value) {
        switch (value) {
            case 0:
                return "NOT_RUNNING";
            case 1:
                return "STARTING";
            case 2:
                return "RECOVERY";
            case 3:
                return "RUNNING";
            case 6:
                return "PENDING_CONTROLLED_SHUTDOWN";
            case 7:
                return "SHUTTING_DOWN";
            default:
                return "UNKNOWN";
        }
    }

    private synchronized void removeBrokerStateRoute() {
        LOGGER.info("Removing route for path={}", BrokerStateResponse.PATH);
        brokerStateRoute.remove();
        brokerStateRoute = null;
    }

    private boolean isZkSessionState(MetricName name) {
        return "SessionExpireListener".equals(name.getType())
                && "kafka.server".equals(name.getGroup())
                && "SessionState".equals(name.getName());
    }

    private synchronized void addZkSessionStateRoute(Gauge<?> gauge) {
        if (zkSessionRoute == null) {
            LOGGER.info("Adding route for path={}", ZkSessionStateResponse.PATH);
            this.zkSessionRoute = router.get(ZkSessionStateResponse.PATH)
                    .respond(ctx -> {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Handling {} {}", ctx.request().method(), ctx.request().path());
                        }
                        String description = String.valueOf(gauge.value());
                        return Future.succeededFuture(new ZkSessionStateResponse(description));
                    });
        }
    }

    private synchronized void removeZkSessionStateRoute() {
        LOGGER.info("Removing route for path={}", ZkSessionStateResponse.PATH);
        zkSessionRoute.remove();
        zkSessionRoute = null;
    }


    // The kubelet uses readiness probes to know when a container is ready to start accepting traffic.
    private synchronized void addReadyRoute(Gauge<? extends Number> gauge) {
        if (readinessRoute == null) {
            LOGGER.info("Adding route for path={}, contentType={}", READY_PATH, TEXT_PLAIN_CONTENT_TYPE);
            this.readinessRoute = router.get(READY_PATH).produces(TEXT_PLAIN_CONTENT_TYPE).handler(ctx -> {
                int value = gauge.value().intValue();
                if (isReady(value)) {
                    ctx.response().setStatusCode(200).end("READY");
                } else {
                    String description = brokerStateDescription(value);
                    ctx.response().setStatusCode(404).end(String.format("NOT READY (broker state = %s)", description));
                }
            });
        }
    }

    private synchronized void removeReadyRoute() {
        LOGGER.info("Removing route for path={}", READY_PATH);
        readinessRoute.remove();
        readinessRoute = null;
    }

    private boolean isReady(int brokerState) {
        return brokerState >= 3;
    }

    // The kubelet uses liveness probes to know when to restart a container.
    private synchronized void addAliveRoute(Gauge<?> zkSession) {
        if (aliveRoute == null) {
            LOGGER.info("Adding route for path={}, contentType={}", ALIVE_PATH, TEXT_PLAIN_CONTENT_TYPE);
            this.aliveRoute = router.get(ALIVE_PATH).produces(TEXT_PLAIN_CONTENT_TYPE).handler(ctx -> {
                int brokerState;
                synchronized (this) {
                    brokerState = this.brokerState.value().intValue();
                }
                if (isReady(brokerState)) {
                    ping.apply(host, port).onSuccess(ar -> {
                        ctx.response().setStatusCode(200).end("ALIVE (broker state >= RUNNING && can connect)");
                    }).onFailure(i -> {
                        ctx.response().setStatusCode(404).end("NOT ALIVE (broker state >= RUNNING but can't connect)");
                    });

                } else {
                    String sessionStateStr = String.valueOf(zkSession.value());
                    if ("CONNECTED".equals(sessionStateStr)) {
                        ctx.response().setStatusCode(200).end("ALIVE (zk session state = CONNECTED)");
                    } else {
                        ctx.response().setStatusCode(404).end(String.format("NOT ALIVE (zk session state = %s)", sessionStateStr));
                    }
                }
            });
        }
    }
    
    private synchronized void removeAliveRoute() {
        LOGGER.info("Removing route for path={}", ALIVE_PATH);
        aliveRoute.remove();
        aliveRoute = null;
    }
}
