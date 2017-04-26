package com.github.thinker0.mesos;

import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;

/**
 * The RouteTable class contains all URL routes in the WebServer.
 */
class RouteTable {
    private final ArrayList<Route> routes;

    RouteTable() {
        this.routes = new ArrayList<Route>();
    }

    public void addRoute(final Route route) {
        this.routes.add(route);
    }

    public Route findRoute(final HttpMethod method, final String path) {
        for (final Route route : routes) {
            if (route.matches(method, path)) {
                return route;
            }
        }

        return null;
    }
}
