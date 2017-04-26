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
