package com.metaring.framework.ext.rpc.vertx;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Consumer;

import com.ea.async.Async;
import com.metaring.framework.Core;
import com.metaring.framework.FrameworkFunctionalitiesManager;
import com.metaring.framework.Resources;
import com.metaring.framework.SysKB;
import com.metaring.framework.Tools;
import com.metaring.framework.broadcast.BroadcastController;
import com.metaring.framework.ext.vertx.VertxUtilities;
import com.metaring.framework.rpc.CallFunctionalityImpl;
import com.metaring.framework.type.DataRepresentation;
import com.metaring.framework.type.series.DigitSeries;
import com.metaring.framework.type.series.TextSeries;
import com.metaring.framework.util.ObjectUtil;
import com.metaring.framework.util.StringUtil;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;

public final class VertxRPCController extends AbstractVerticle {
    public static final String STANDARD_REQUEST_TEMPLATE = "{\"id\":%d, \"name\":\"%s\", \"ipAddress\":\"%s\", \"param\":\"%s\"}";
    protected static final String CFG_WEB_SERVICES = "webServices";
    protected static final String CFG_CORS = "cors";
    protected static final String CFG_CORS_ALLOWED_ORIGIN_PATTERN = "allowedOriginPattern";
    protected static final String CFG_CORS_ALLOWED_HEADERS = "allowedHeaders";
    protected static final String CFG_CORS_ALLOWED_METHODS = "allowedMethods";
    protected static final String CFG_HTTPSERVER_OPTIONS = "httpServerOptions";
    protected static final String CFG_SSL = "ssl";
    protected static final String CFG_HTTP_PORTS_TO_REDIRECT = "HTTPPortsToRedirect";
    protected static final String CFG_WEB_SERVICES_ACCESS_POINT_NAME = "accessPointName";
    protected static final String CFG_WEB_SERVICES_ACCESS_POINT_PORT = "accessPointPort";
    protected static final String CFG_WEB_SERVICES_REST = "rest";
    protected static final String CFG_WEB_SERVICES_SOCKJS = "sockjs";

    private static String REST_REDIRECT_FORMAT = "http";

    private static ComplementaryRoutingAction COMPLEMENTARY_ROUTING_ACTION;
    private static Runnable ON_START;

    public static final void run(String[] args, ComplementaryRoutingAction complementaryRoutingAction, Runnable onStart) {
        COMPLEMENTARY_ROUTING_ACTION = complementaryRoutingAction;
        ON_START = onStart;
        SysKB sysKB = Core.SYSKB;
        ClusterManager.CONSUMERS.isEmpty();
        int instances = 1;
        if (sysKB.hasProperty(Resources.CFG_EXT)) {
            DataRepresentation ext = sysKB.get(Resources.CFG_EXT);
            if (ext.hasProperty(VertxUtilities.CFG_VERTX)) {
                DataRepresentation vertx = ext.get(VertxUtilities.CFG_VERTX);
                if (vertx.hasProperty(VertxUtilities.CFG_INSTANCES)) {
                    instances = vertx.getDigit(VertxUtilities.CFG_INSTANCES).intValue();
                }
            }
        }
        VertxUtilities.INSTANCE.deployVerticle(VertxRPCController.class.getName(), new DeploymentOptions().setInstances(instances));
    }

    public static final void run(String[] args) {
        run(args, null, null);
    }

    public static final void run(String[] args, ComplementaryRoutingAction complementaryRoutingAction) {
        run(args, complementaryRoutingAction, null);
    }

    public static final void run(String[] args, Runnable onStart) {
        run(args, null, onStart);
    }

    public static final void main(String[] args) {
        run(args, null, null);
    }

    @Override
    public final void start() throws Exception {
        String servicesAccessPointName;
        DataRepresentation vertxWebServicesConfiguration = Core.SYSKB.get(Resources.CFG_EXT).get(VertxUtilities.CFG_VERTX).get(CFG_WEB_SERVICES);
        if (!(servicesAccessPointName = vertxWebServicesConfiguration.getText(CFG_WEB_SERVICES_ACCESS_POINT_NAME)).startsWith("/")) {
            servicesAccessPointName = "/" + servicesAccessPointName;
        }
        if (servicesAccessPointName.endsWith("/")) {
            servicesAccessPointName.substring(0, servicesAccessPointName.length() - 1);
        }
        int servicesAccessPointPort = vertxWebServicesConfiguration.getDigit(CFG_WEB_SERVICES_ACCESS_POINT_PORT).intValue();
        Router router = Router.router(vertx);
        tryAddCorsHandlerInfo(router, vertxWebServicesConfiguration);
        router.route().handler(BodyHandler.create());
        if (vertxWebServicesConfiguration.hasProperty(CFG_WEB_SERVICES_SOCKJS) && vertxWebServicesConfiguration.getTruth(CFG_WEB_SERVICES_SOCKJS)) {
            router.route(servicesAccessPointName + ".sock/*").handler(SockJSHandler.create(vertx).socketHandler(this::consumeSockJsRequest));
        }
        if (vertxWebServicesConfiguration.hasProperty(CFG_WEB_SERVICES_REST) && vertxWebServicesConfiguration.getTruth(CFG_WEB_SERVICES_REST)) {
            router.post(servicesAccessPointName).handler(this::consumePostRequest);
        }
        if(COMPLEMENTARY_ROUTING_ACTION != null) {
            COMPLEMENTARY_ROUTING_ACTION.attach(servicesAccessPointName, vertxWebServicesConfiguration, router);
            COMPLEMENTARY_ROUTING_ACTION = null;
        }
        HttpServerOptions serverOptions = new HttpServerOptions();
        final DigitSeries HTTPPortsToRedirect = Tools.FACTORY_DIGIT_SERIES.create();
        try {
            if (vertxWebServicesConfiguration.hasProperty(CFG_HTTPSERVER_OPTIONS)) {
                serverOptions = new HttpServerOptions(new JsonObject(vertxWebServicesConfiguration.get(CFG_HTTPSERVER_OPTIONS).toJson()));
                DataRepresentation httpServerOptions = vertxWebServicesConfiguration.get(CFG_HTTPSERVER_OPTIONS);
                if(httpServerOptions.isTruth(CFG_SSL)) {
                    REST_REDIRECT_FORMAT += "s";
                }
                REST_REDIRECT_FORMAT  += "://%s:" + servicesAccessPointPort + "/" + servicesAccessPointName;
                if(httpServerOptions.isTruth(CFG_SSL) && httpServerOptions.getTruth(CFG_SSL) && httpServerOptions.isDigitSeries(CFG_HTTP_PORTS_TO_REDIRECT)) {
                    HTTPPortsToRedirect.addAll(httpServerOptions.getDigitSeries(CFG_HTTP_PORTS_TO_REDIRECT));
                }
            }
        }
        catch (Exception e) {
        }
        HttpServerOptions httpServerOptions = serverOptions.setHost("0.0.0.0").setPort(servicesAccessPointPort);
        this.vertx.executeBlocking(f1 -> {
            Async.init();
            try {
                FrameworkFunctionalitiesManager.reinit().handle((result, error) -> {
                    this.vertx.executeBlocking(f2 -> {
                        try {
                            this.vertx.createHttpServer(httpServerOptions).requestHandler(router::accept).listen();
                            tryRedirectStandardHTTPRequests(HTTPPortsToRedirect, servicesAccessPointPort);
                            if(ON_START != null) {
                                ON_START.run();
                            }
                            f2.complete();
                        } catch(Exception e) {
                            f2.fail(e);
                        }
                    }, f2Result -> {
                        if(f2Result.failed()) {
                            f1.fail(f2Result.cause());
                            return;
                        }
                        f1.complete();
                    });
                    return null;
                });
            } catch(Exception e) {
                f1.fail(e);
            }
        }, null);
    }

    private static void tryAddCorsHandlerInfo(Router router, DataRepresentation vertxWebServicesConfiguration) {
        if(ObjectUtil.isNullOrEmpty(vertxWebServicesConfiguration.get(CFG_CORS)) || (vertxWebServicesConfiguration.isTruth(CFG_CORS) && !vertxWebServicesConfiguration.getTruth(CFG_CORS))) {
            return;
        }
        DataRepresentation cors = vertxWebServicesConfiguration.get(CFG_CORS);
        String allowedOriginPattern = cors.getText(CFG_CORS_ALLOWED_ORIGIN_PATTERN);
        CorsHandler corsHandler = CorsHandler.create(StringUtil.isNullOrEmpty(allowedOriginPattern) ? "*" : allowedOriginPattern);
        TextSeries allowedHeaders = cors.getTextSeries(CFG_CORS_ALLOWED_HEADERS);
        if(!ObjectUtil.isNullOrEmpty(allowedHeaders)) {
            corsHandler.allowedHeaders(new HashSet<>(allowedHeaders));
        }
        TextSeries allowedMethods = cors.getTextSeries(CFG_CORS_ALLOWED_METHODS);
        if(!ObjectUtil.isNullOrEmpty(allowedMethods)) {
            corsHandler.allowedMethods(new HashSet<>(Arrays.asList(allowedMethods.stream().map(HttpMethod::valueOf).toArray(HttpMethod[]::new))));
        }
        router.route().handler(corsHandler);
    }

    private final void tryRedirectStandardHTTPRequests(DigitSeries HTTPPortsToRedirect, int servicesAccessPointPort) {
        if(HTTPPortsToRedirect == null || HTTPPortsToRedirect.isEmpty()) {
            return;
        }
        final Handler<RoutingContext> requestHandler = context -> {
            try {
                URI currentURI = new URI(context.request().absoluteURI());
                String scheme = currentURI.getScheme().contains("ws") ? "wss" : "https";
                URI redirectURI = new URI(scheme,
                    currentURI.getUserInfo(),
                    currentURI.getHost(),
                    servicesAccessPointPort,
                    currentURI.getRawPath(),
                    currentURI.getRawQuery(),
                    currentURI.getRawFragment());
                    context.response().putHeader("Location", redirectURI.toString()).setStatusCode(302).end();
            } catch (Exception e) {
                e.printStackTrace();
                context.fail(404);
            }
        };
        final Router router = Router.router(this.vertx);
        router.route().handler(requestHandler);
        HTTPPortsToRedirect.forEach(it -> this.vertx.createHttpServer().requestHandler(router::accept).listen(it.intValue()));
    }

    private final void consumePostRequest(RoutingContext routingContext) {
        ClusterManager.connected().thenAcceptAsync(connection -> {
            if(connection != null) {
                routingContext.response().putHeader("Location", String.format(REST_REDIRECT_FORMAT, connection)).setStatusCode(307).end();
                return;
            }
            runAsync(new RequestData(routingContext).setStateless(), response -> {
                ClusterManager.disconnected();
                routingContext.response().end(response);
            });
        });
    }

    private final void consumeSockJsRequest(final SockJSSocket sockJSSocket) {
        ClusterManager.connected().thenAcceptAsync(connection -> {
            if(connection != null) {
                sockJSSocket.write("Location=" + connection).close();
                return;
            }
            final RequestData requestData = new RequestData(sockJSSocket, tryAttachBroadcastComponents(sockJSSocket));
            sockJSSocket.handler(buffer -> {
                runAsync(requestData.copy(buffer), response -> {
                    this.vertx.runOnContext(v -> {
                        sockJSSocket.write(response);
                    });
                });
            });
        }, VertxUtilities.INSTANCE_AS_EXECUTOR);
    }

    public static final void runAsync(RequestData requestData, Consumer<Buffer> responseCallback) {
        CallFunctionalityImpl.execute(requestData.id, requestData.sourceIp, BroadcastController.BROADCAST_KEY, requestData.broadcastKey, requestData.stateless, requestData.getJsonPayload()).whenCompleteAsync((result, error) -> {
            if(error != null) {
                error.printStackTrace();
                return;
            }
            responseCallback.accept(Buffer.buffer(result.getResult().toJson()));
        }, VertxUtilities.INSTANCE_AS_EXECUTOR);
    }

    private final String tryAttachBroadcastComponents(final SockJSSocket sockJSSocket) {
        final String key = sockJSSocket.remoteAddress().host() + "@" + System.currentTimeMillis() * Math.random();
        sockJSSocket.endHandler(v -> {
            ClusterManager.disconnected();
            BroadcastController.unregister(key);
        });
        BroadcastController.register(key, message -> {
            try {
                sockJSSocket.write(Buffer.buffer(message));
            } catch(Exception e) {
                e.printStackTrace();
            }
        });
        return key;
    }

    @FunctionalInterface
    public static interface ComplementaryRoutingAction {
        void attach(String servicesAccessPointName, DataRepresentation vertxWebServicesConfiguration, Router router);
    }
}
