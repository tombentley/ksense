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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.web.Router;
import kafka.server.KafkaConfig$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reference counted holder for a common Vertx {@code HttpServer} and {@code Router}, which can be shared between multiple broker plugins
 * each adding and removing their own Routes.
 * When a plugin has routes to add it should call {@link #instance(Map)} to get (and possibly initialize) the shared instance.
 * When a plugin removes its last route it should call {@link #close()}.
 * The {@code HttpServer} and {@code Vertx} instance get closed when the reference count reaches zero.
 */
public class RouterHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterHolder.class);

    private static RouterHolder INSTANCE = null;
    private int refCount; // guarded by this object's monitor
    private final Vertx vertx;
    private final Router router;
    private final HttpServer server;

    public static synchronized RouterHolder instance(Map<String, String> config) {
        if (INSTANCE == null) {
            return INSTANCE = new RouterHolder(config);
        } else {
            synchronized (INSTANCE) {
                if (INSTANCE.refCount > 1) {
                    ++INSTANCE.refCount;
                    return INSTANCE;
                } else if (INSTANCE.refCount == 0) {
                    // A previously close instance => allocate a new one
                    return INSTANCE = new RouterHolder(config);
                } else {
                    throw new IllegalStateException("Illegal ref count " + INSTANCE.refCount);
                }

            }
        }
    }

    private RouterHolder(Map<String, String> config) {
        int port;
        String s = config.get("ksense.port");
        if (s != null) {
            port = Integer.parseInt(s);
        } else {
            // default to 8080, or relative offset from 9092 (e.g. listeners is 9093 then 8081)
            String listenersProp = config.getOrDefault("listeners",
                    (String) KafkaConfig$.MODULE$.configDef().defaultValues().get("listeners"));// PLAINTEXT://:9092
            port = Integer.parseInt(listenersProp.substring(listenersProp.lastIndexOf(":") + 1)) - 9092 + 8080;
        }
        this.vertx = Vertx.vertx();
        this.server = vertx.createHttpServer(new HttpServerOptions().setPort(port));
        this.router = Router.router(vertx);
        var future = new CompletableFuture<Void>();
        server.requestHandler(router).listen(r -> {
            if (r.succeeded()) {
                future.complete(null);
            } else {
                future.completeExceptionally(r.cause());
            }
        });
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        LOGGER.info("Listening: port={}", port);
        this.refCount = 1;
    }

    public Router router() {
        return router;
    }

    public Future<Void> tcpConnect(String host, int port) {
        Promise<Void> p = Promise.promise();
        vertx.createNetClient(new NetClientOptions().setConnectTimeout(10_000)).connect(port, host, r -> {
            if (r.succeeded()) {
                r.result().close();
                p.complete();
            } else {
                p.fail(r.cause());
            }
        });
        return p.future();
    }

    /**
     * Decrement the reference count, possible closing the vertx instance.
     */
    public synchronized void close() {
        refCount -= 1;
        if (refCount == 0) {
            LOGGER.debug("Actually closing because ref count == 0");
            CountDownLatch l = new CountDownLatch(2);
            server.close(event -> l.countDown());
            vertx.close(event -> l.countDown());
            try {
                l.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                LOGGER.debug("Give up waiting for vertx to shutdown, due to interruption");
            }
        } else if (refCount > 0) {
            LOGGER.debug("Decremented ref count to {}", refCount);
        } else {
            throw new IllegalStateException("Illegal ref count " + refCount);
        }
    }
}
