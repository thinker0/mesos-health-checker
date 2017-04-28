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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * The WebServer class is a convenience wrapper around the Netty HTTP server.
 */
class MesosHealthCheckerServer implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    public static final String TYPE_PLAIN = "text/plain; charset=UTF-8";
    public static final String TYPE_JSON = "application/json; charset=UTF-8";
    public static final String SERVER_NAME = "Netty";
    private final RouteTable routeTable;
    private final int port;
    EventLoopGroup eventLoopGroup;


    /**
     * Creates a new WebServer.
     */
    MesosHealthCheckerServer(Integer port) {
        this.routeTable = new RouteTable();
        this.port = port;
    }

    /**
     * Writes a 404 Not Found response.
     *
     * @param ctx     The channel context.
     * @param request The HTTP request.
     */
    private void writeNotFound(
        final ChannelHandlerContext ctx,
        final FullHttpRequest request) {

        writeErrorResponse(ctx, request, HttpResponseStatus.NOT_FOUND);
    }

    /**
     * Writes a 500 Internal Server Error response.
     *
     * @param ctx     The channel context.
     * @param request The HTTP request.
     */
    private void writeInternalServerError(
        final ChannelHandlerContext ctx,
        final FullHttpRequest request) {

        writeErrorResponse(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Writes a HTTP error response.
     *
     * @param ctx     The channel context.
     * @param request The HTTP request.
     * @param status  The error status.
     */
    private void writeErrorResponse(
        final ChannelHandlerContext ctx,
        final FullHttpRequest request,
        final HttpResponseStatus status) {

        writeResponse(ctx, request, status, TYPE_PLAIN, status.reasonPhrase());
    }

    /**
     * Writes a HTTP response.
     *
     * @param ctx         The channel context.
     * @param request     The HTTP request.
     * @param status      The HTTP status code.
     * @param contentType The response content type.
     * @param content     The response content.
     */
    private void writeResponse(
        final ChannelHandlerContext ctx,
        final FullHttpRequest request,
        final HttpResponseStatus status,
        final CharSequence contentType,
        final String content) {

        final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        final ByteBuf entity = Unpooled.wrappedBuffer(bytes);
        writeResponse(ctx, request, status, entity, contentType, bytes.length);
    }

    /**
     * Writes a HTTP response.
     *
     * @param ctx           The channel context.
     * @param request       The HTTP request.
     * @param status        The HTTP status code.
     * @param buf           The response content buffer.
     * @param contentType   The response content type.
     * @param contentLength The response content length;
     */
    private void writeResponse(
        final ChannelHandlerContext ctx,
        final FullHttpRequest request,
        final HttpResponseStatus status,
        final ByteBuf buf,
        final CharSequence contentType,
        final int contentLength) {

        // Decide whether to close the connection or not.
        final boolean keepAlive = isKeepAlive(request);

        // Build the response object.
        final FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            status,
            buf,
            false);

        final ZonedDateTime dateTime = ZonedDateTime.now();
        final DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;

        final DefaultHttpHeaders headers = (DefaultHttpHeaders) response.headers();
        headers.set(HttpHeaderNames.SERVER, SERVER_NAME);
        headers.set(HttpHeaderNames.DATE, dateTime.format(formatter));
        headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
        headers.set(HttpHeaderNames.CONTENT_LENGTH, Integer.toString(contentLength));

        // Close the non-keep-alive connection after the write operation is done.
        if (!keepAlive) {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } else {
            ctx.writeAndFlush(response, ctx.voidPromise());
        }
    }

    public boolean is100ContinueExpected(HttpMessage message) {
        if (!(message instanceof HttpRequest)) {
            return false;
        } else if (message.protocolVersion().compareTo(HttpVersion.HTTP_1_1) < 0) {
            return false;
        } else {
            CharSequence value = (CharSequence) message.headers().get(HttpHeaderNames.EXPECT);
            return value == null ? false : (HttpHeaderValues.CONTINUE.contentEqualsIgnoreCase(value) ? true : message.headers().contains(HttpHeaderNames.EXPECT, HttpHeaderValues.CONTINUE, true));
        }
    }

    public boolean isKeepAlive(HttpMessage message) {
        CharSequence connection = (CharSequence) message.headers().get(HttpHeaderNames.CONNECTION);
        return connection != null && HttpHeaderValues.CLOSE.contentEqualsIgnoreCase(connection) ? false : (message.protocolVersion().isKeepAliveDefault() ? !HttpHeaderValues.CLOSE.contentEqualsIgnoreCase(connection) : HttpHeaderValues.KEEP_ALIVE.contentEqualsIgnoreCase(connection));
    }

    /**
     * Writes a 100 Continue response.
     *
     * @param ctx The HTTP handler context.
     */
    private static void send100Continue(final ChannelHandlerContext ctx) {
        ctx.write(new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.CONTINUE));
    }

    /**
     * Adds a GET route.
     *
     * @param path    The URL path.
     * @param handler The request handler.
     * @return This WebServer.
     */
    public MesosHealthCheckerServer get(final String path, final Handler handler) {
        this.routeTable.addRoute(new Route(HttpMethod.GET, path, handler));
        return this;
    }

    /**
     * Adds a POST route.
     *
     * @param path    The URL path.
     * @param handler The request handler.
     * @return This WebServer.
     */
    public MesosHealthCheckerServer post(final String path, final Handler handler) {
        this.routeTable.addRoute(new Route(HttpMethod.POST, path, handler));
        return this;
    }

    /**
     * Starts the web server.
     *
     * @throws Exception
     */
    public EventLoopGroup start() throws Exception {
        if (Epoll.isAvailable()) {
            eventLoopGroup = new EpollEventLoopGroup();
            start(new EpollEventLoopGroup(), EpollServerSocketChannel.class);
        } else {
            eventLoopGroup = new NioEventLoopGroup();
            start(new NioEventLoopGroup(), NioServerSocketChannel.class);
        }
        return eventLoopGroup;
    }

    /**
     * Shutdown the web server.
     */
    public void close() {
        try {
            eventLoopGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * Initializes the server, socket, and channel.
     *
     * @param loopGroup          The event loop group.
     * @param serverChannelClass The socket channel class.
     * @throws InterruptedException on interruption.
     */
    private void start(
        final EventLoopGroup loopGroup,
        final Class<? extends ServerChannel> serverChannelClass)
        throws InterruptedException {

        try {
            final InetSocketAddress inet = new InetSocketAddress(port);

            final ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.option(ChannelOption.SO_REUSEADDR, true);
            b.option(ChannelOption.SO_LINGER, 0);
            b.group(loopGroup).channel(serverChannelClass).childHandler(new WebServerInitializer());
            b.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));
            b.childOption(ChannelOption.SO_REUSEADDR, true);

            // final Channel ch = b.bind(inet).sync().channel();
            // ch.closeFuture().sync();
            b.bind(inet);
            logger.info("Listening for Admin on {}", inet);
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        } finally {
            // loopGroup.shutdownGracefully().sync();
        }
    }

    /**
     * The Initializer class initializes the HTTP channel.
     */
    private class WebServerInitializer extends ChannelInitializer<SocketChannel> {

        /**
         * Initializes the channel pipeline with the HTTP response handlers.
         *
         * @param ch The Channel which was registered.
         */
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            final ChannelPipeline p = ch.pipeline();
            p.addLast("decoder", new HttpRequestDecoder(4096, 8192, 8192, false));
            p.addLast("aggregator", new HttpObjectAggregator(100 * 1024 * 1024));
            p.addLast("encoder", new HttpResponseEncoder());
            p.addLast("handler", new WebServerHandler());
        }
    }

    /**
     * The Handler class handles all inbound channel messages.
     */
    private class WebServerHandler extends SimpleChannelInboundHandler<Object> {

        /**
         * Netty 5.x
         *
         * @param ctx
         * @param msg
         */
        public void messageReceived(final ChannelHandlerContext ctx, final Object msg) {
            channelRead0(ctx, msg);
        }

        /**
         * Handles a new message.
         *
         * @param ctx The channel context.
         * @param msg The HTTP request message.
         *            <p>
         *            TODO 5.x channelRead0 -> messageReceived
         */
        @Override
        public void channelRead0(final ChannelHandlerContext ctx, final Object msg) {
            if (!(msg instanceof FullHttpRequest)) {
                return;
            }

            final FullHttpRequest request = (FullHttpRequest) msg;

            if (is100ContinueExpected(request)) {
                send100Continue(ctx);
            }

            final HttpMethod method = request.method();
            final String uri = request.uri();

            final Route route = MesosHealthCheckerServer.this.routeTable.findRoute(method, uri);
            if (route == null) {
                writeNotFound(ctx, request);
                return;
            }

            try {
                final Response response = route.getHandler().handle();
                writeResponse(ctx, request, HttpResponseStatus.valueOf(response.getStatus()), TYPE_PLAIN, response.getMessage());
            } catch (final Exception ex) {
                logger.warn(ex.getMessage(), ex);
                writeInternalServerError(ctx, request);
            }
        }


        /**
         * Handles an exception caught.  Closes the context.
         *
         * @param ctx   The channel context.
         * @param cause The exception.
         */
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            ctx.close();
        }


        /**
         * Handles read complete event.  Flushes the context.
         *
         * @param ctx The channel context.
         */
        @Override
        public void channelReadComplete(final ChannelHandlerContext ctx) {
            ctx.flush();
        }
    }
}
