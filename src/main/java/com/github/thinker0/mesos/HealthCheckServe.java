package com.github.thinker0.mesos;

import io.netty.channel.EventLoopGroup;

import java.io.Closeable;

/**
 * https://github.com/codyebberson/netty-example
 */
public class HealthCheckServe implements Closeable {
    EventLoopGroup server;

    public void start() {
        final int port = Integer.parseInt(System.getProperty("admin.port", "9990"));

        try {
            server = new MesosHealthCheckerServer(port)
                // health request
                .get("/health", (request, response) -> "OK")

                // quitquitquit request
                .post("/quitquitquit", (request, response) -> {
                    return "OK";
                })

                // abortabortabort handling
                .get("/abortabortabort", (request, response) -> {
                    this.close();
                    return "OK";
                })

                // Start the server
                .start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        server.shutdownGracefully();
    }
}
