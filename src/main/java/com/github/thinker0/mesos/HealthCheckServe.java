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

/**
 * https://github.com/codyebberson/netty-example
 */
public class HealthCheckServe implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Runnable quit;
    private final Runnable abort;
    EventLoopGroup server;

    public HealthCheckServe() {
        this(() -> {}, () -> {});
    }

    public HealthCheckServe(Runnable quit, Runnable abort) {
        this.quit = quit;
        this.abort = abort;
    }

    public void start() {
        final int port = Integer.parseInt(System.getProperty("admin.port", "9990"));

        try {
            server = new MesosHealthCheckerServer(port)
                // health request
                .get("/health", (request, response) -> "OK")

                // quitquitquit request
                .post("/quitquitquit", (request, response) -> {
                    quit.run();
                    return "OK";
                })

                // abortabortabort handling
                .get("/abortabortabort", (request, response) -> {
                    abort.run();
                    this.close();
                    return "OK";
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
