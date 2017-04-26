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

import io.netty.handler.codec.http.HttpMethod;

/**
 * The Route class represents a single entry in the RouteTable.
 */
class Route {
    private final HttpMethod method;
    private final String path;
    private final Handler handler;

    Route(final HttpMethod method, final String path, final Handler handler) {
        this.method = method;
        this.path = path;
        this.handler = handler;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public String getPath() {
        return path;
    }

    public Handler getHandler() {
        return handler;
    }

    public boolean matches(final HttpMethod method, final String path) {
        return this.method.equals(method) && this.path.equals(path);
    }
}


