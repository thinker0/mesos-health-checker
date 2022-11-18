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

import io.prometheus.client.exporter.HTTPServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * https://github.com/codyebberson/netty-example
 */
public class HealthCheckServe extends HTTPServer implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Set<BooleanSupplier> healthCheckers = new HashSet<>();
    private final Optional<Runnable> quit;
    private final Optional<Runnable> abort;
    private final String body;

    public HealthCheckServe() throws IOException {
        this(9990);
    }

    public HealthCheckServe(int port) throws IOException {
        this(port, "OK", () -> {
        });
    }

    public HealthCheckServe(int port, String ok) throws IOException {
        this(port, ok, () -> {
        });
    }

    public HealthCheckServe(int port, String ok, Runnable quit) throws IOException {
        this(port, ok, quit, () -> {
        });
    }

    public HealthCheckServe(int port, String ok, Runnable quit, Runnable abort) throws IOException {
        super(port, false);
        this.body = ok;
        this.quit = Optional.of(quit);
        this.abort = Optional.of(abort);
        logger.info("Starting health check server on port {}", port);
        server.createContext("/health", httpExchange -> {
            final Optional<BooleanSupplier> health = health();
            if (health.isPresent()) {
                httpExchange.sendResponseHeaders(500, 0);
                httpExchange.getResponseBody().write(health.get().toString().getBytes());
            } else {
                byte[] bodyBytes = body.getBytes();
                httpExchange.sendResponseHeaders(200, bodyBytes.length);
                httpExchange.getResponseBody().write(bodyBytes);
            }
            httpExchange.close();
        });
        server.createContext("/quitquitquit", httpExchange -> {
            httpExchange.sendResponseHeaders(200, 0);
            httpExchange.getResponseBody().write("OK".getBytes());
            httpExchange.close();
            quitQuitQuit();
        });
        server.createContext("/abortabortabort", httpExchange -> {
            httpExchange.sendResponseHeaders(200, 0);
            httpExchange.getResponseBody().write("OK".getBytes());
            httpExchange.close();
            abortAbortAbort();
        });
    }

    public HealthCheckServe addHealthCheck(BooleanSupplier checker) {
        healthCheckers.add(checker);
        return this;
    }

    public Optional<BooleanSupplier> health() {
        return healthCheckers.stream().parallel().filter(b -> !b.getAsBoolean()).findFirst();
    }

    public HealthCheckServe quitQuitQuit() {
        this.quit.ifPresent(Runnable::run);
        return this;
    }

    public HealthCheckServe abortAbortAbort() {
        this.abort.ifPresent(Runnable::run);
        return this;
    }
}
