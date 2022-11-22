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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Predicate;
import io.prometheus.client.SampleNameFilter;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.common.TextFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.zip.GZIPOutputStream;

import com.sun.net.httpserver.HttpHandler;

/**
 * new HealthCheckServe(8080, 8081, () -> true);
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
        final CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        server.createContext("/metrics", httpExchange -> {
            String query = httpExchange.getRequestURI().getRawQuery();
            String contextPath = httpExchange.getHttpContext().getPath();
            ByteArrayOutputStream response = new ByteArrayOutputStream(1 << 20);
            response.reset();
            OutputStreamWriter osw = new OutputStreamWriter(response, StandardCharsets.UTF_8);
            String contentType = TextFormat.chooseContentType(httpExchange.getRequestHeaders().getFirst("Accept"));
            httpExchange.getResponseHeaders().set("Content-Type", contentType);
            Predicate<String> filter = null;
            filter = SampleNameFilter.restrictToNamesEqualTo(filter, parseQuery(query));
            if (filter == null) {
                TextFormat.writeFormat(contentType, osw, registry.metricFamilySamples());
            } else {
                TextFormat.writeFormat(contentType, osw, registry.filteredMetricFamilySamples(filter));
            }
            osw.flush();
            osw.close();

            if (shouldUseCompression(httpExchange)) {
                httpExchange.getResponseHeaders().set("Content-Encoding", "gzip");
                httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                final GZIPOutputStream os = new GZIPOutputStream(httpExchange.getResponseBody());
                try {
                    response.writeTo(os);
                } finally {
                    os.close();
                }
            } else {
                long contentLength = response.size();
                if (contentLength > 0) {
                    httpExchange.getResponseHeaders().set("Content-Length", String.valueOf(contentLength));
                }
                if (httpExchange.getRequestMethod().equals("HEAD")) {
                    contentLength = -1;
                }
                httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, contentLength);
                response.writeTo(httpExchange.getResponseBody());
            }
            httpExchange.close();
        });
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
            if ("POST".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                httpExchange.sendResponseHeaders(200, 0);
                httpExchange.getResponseBody().write("OK".getBytes());
                httpExchange.close();
                quitQuitQuit();
            } else {
                httpExchange.sendResponseHeaders(404, 0);
            }
            httpExchange.close();
        });
        server.createContext("/abortabortabort", httpExchange -> {
            if ("POST".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                httpExchange.sendResponseHeaders(200, 0);
                httpExchange.getResponseBody().write("OK".getBytes());
                abortAbortAbort();
            } else {
                httpExchange.sendResponseHeaders(404, 0);
            }
            httpExchange.close();
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
