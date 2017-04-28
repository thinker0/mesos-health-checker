/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.thinker0.mesos;

import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * https://github.com/codyebberson/netty-example
 */
public class HealthCheckServe implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Set<BooleanSupplier> healthCheckers = new HashSet<>();
    private final Optional<Runnable> quit;
    private final Optional<Runnable> abort;
    private final String body;
    EventLoopGroup server;

    public HealthCheckServe() {
        this("", () -> {
        });
    }

    public HealthCheckServe(String ok) {
        this(ok, () -> {
        });
    }

    public HealthCheckServe(String ok, Runnable quit) {
        this(ok, quit, () -> {
        });
    }

    public HealthCheckServe(String ok, Runnable quit, Runnable abort) {
        this.body = ok;
        this.quit = Optional.of(quit);
        this.abort = Optional.of(abort);
    }

    public HealthCheckServe addHealthCheck(BooleanSupplier checker) {
        healthCheckers.add(checker);
        return this;
    }

    public Long health() {
        return healthCheckers.stream().parallel().filter(b -> !b.getAsBoolean()).count();
    }

    public HealthCheckServe quitQuitQuit() {
        this.quit.ifPresent(Runnable::run);
        return this;
    }

    public HealthCheckServe abortAbortAbort() {
        this.abort.ifPresent(Runnable::run);
        return this;
    }

    public void start() {
        final int port = Integer.parseInt(System.getProperty("admin.port", "9990"));

        try {
            server = new MesosHealthCheckerServer(port)
                // health request
                .get("/health", () -> {
                    if (health() > 0) {
                        return new Response(500, "ERROR");
                    }
                    return new Response(200, "OK");
                })

                // quitquitquit request
                .post("/quitquitquit", () -> {
                    this.quitQuitQuit();
                    return new Response(200, "OK");
                })

                // abortabortabort handling
                .get("/abortabortabort", () -> {
                    this.abortAbortAbort();
                    this.close();
                    return new Response(200, "OK");
                })

                // Start the server
                .start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void close() {
        server.shutdownGracefully();
    }
}
