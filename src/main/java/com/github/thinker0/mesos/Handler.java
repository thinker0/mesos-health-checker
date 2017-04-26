package com.github.thinker0.mesos;

public interface Handler {

    Object handle(Request request, Response response) throws Exception;

}
