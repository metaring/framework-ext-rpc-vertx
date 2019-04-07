package com.metaring.framework.ext.rpc.vertx;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;
import com.metaring.framework.util.CryptoUtil;

public class RequestData {
    Long id = (long) (System.currentTimeMillis() * Math.random());
    String sourceIp;
    String broadcastKey;
    private Buffer buffer;
    boolean stateless = false;

    private RequestData(String sourceIp, String broadcastKey, Buffer buffer) {
        this.sourceIp = sourceIp;
        this.broadcastKey = broadcastKey;
        this.buffer = buffer;
    }

    RequestData(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public RequestData(RoutingContext routingContext) {
        this(routingContext.request().remoteAddress().host());
        this.buffer = routingContext.getBody();
    }

    RequestData(SockJSSocket sockJSSocket, String broadcastKey) {
        this(sockJSSocket.remoteAddress().host());
        this.broadcastKey = broadcastKey;
    }

    RequestData(SockJSSocket sockJSSocket) {
        this(sockJSSocket, null);
    }

    public final Long getId() {
        return id;
    }

    public final RequestData setStateless() {
        this.stateless = true;
        return this;
    }

    public final String getSourceIp() {
        return sourceIp;
    }

    public final String getJsonPayload() {
        return new String(this.buffer.getBytes(), CryptoUtil.CHARSET_UTF_8);
    }

    public final Buffer buffer() {
        Buffer buffer = this.buffer;
        this.buffer = null;
        return buffer;
    }

    public final RequestData copy(Buffer buffer) {
        return new RequestData(this.sourceIp, this.broadcastKey, buffer);
    }
}