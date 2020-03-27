package com.metaring.framework.ext.rpc.vertx;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.metaring.framework.Tools;
import com.metaring.framework.broadcast.BroadcastController;
import com.metaring.framework.broadcast.Event;
import com.metaring.framework.broadcast.MultipleCallback;
import com.metaring.framework.broadcast.SingleCallback;
import com.metaring.framework.ext.vertx.VertxUtilities;
import com.metaring.framework.type.DataRepresentation;
import com.metaring.framework.util.StringUtil;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class ClusterHandler {

    private static final CompletableFuture<Vertx> INSTANCE = new CompletableFuture<>();
    private static final Map<String, Long> CLUSTERS = new HashMap<>();
    public static final CompletableFuture<Consumer<ClusterMessage>> CONSUMER = new CompletableFuture<>();

    private static String NODE_ID;

    private static long connections = 0;

    static {
        final HazelcastClusterManager[] manager = {null};
        try {
            manager[0] = StringUtil.isNullOrEmpty(ClusterHandler.class.getClassLoader().getResource("cluster.xml").toString()) ? null : new HazelcastClusterManager();
        } catch (Exception e) {
        }
        if (manager[0] == null) {
            INSTANCE.complete(null);
        } else {
            Vertx.clusteredVertx(new VertxOptions().setClusterManager(manager[0]), cluster -> {
                if (cluster.succeeded()) {
                    Vertx vertx = cluster.result();
                    vertx.eventBus().consumer("message", ClusterHandler::handle);
                    BroadcastController.register((type, element) -> publish("broadcast", Tools.FACTORY_DATA_REPRESENTATION.create().add("type", type).add("element", element)));
                    NODE_ID = manager[0].getNodeID() + "_" + (manager[0].getHazelcastInstance().getLocalEndpoint().getSocketAddress().toString().replace("/", "").split(":")[0]);
                    INSTANCE.complete(vertx);
                    publish("sync", connections);
                } else {
                    INSTANCE.completeExceptionally(cluster.cause());
                }
            });
        }
    }

    private static final void handle(Message<String> message) {
        final ClusterMessage clusterMessage = ClusterMessage.fromJson(message.body());
        if(NODE_ID.equalsIgnoreCase(clusterMessage.ID)) {
            return;
        }
        try {
            Method method = ClusterHandler.class.getDeclaredMethod(clusterMessage.topic, String.class, DataRepresentation.class);
            method.setAccessible(true);
            method.invoke(null, clusterMessage.ID, clusterMessage.data);
        } catch (Exception e) {
        }
        CONSUMER.thenAcceptAsync(it -> {
            if (it == null) {
                return;
            }
            try {
                it.accept(clusterMessage);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, VertxUtilities.INSTANCE_AS_EXECUTOR);
    }

    @SuppressWarnings("unused")
    private static final void sync(String caller, DataRepresentation dataRep) {
        CLUSTERS.put(caller, dataRep.asDigit());
        publish("connections", connections);
    }

    @SuppressWarnings("unused")
    private static final void connections(String caller, DataRepresentation dataRep) {
        CLUSTERS.put(caller, dataRep.asDigit());
        System.out.println(CLUSTERS);
    }

    @SuppressWarnings("unused")
    private static final void broadcast(String caller, DataRepresentation dataRep) {
        System.out.println(dataRep);
        String type = dataRep.getText("type");
        if (type == "singleCallback") {
            BroadcastController.callback(dataRep.get("element").as(SingleCallback.class), VertxUtilities.INSTANCE_AS_EXECUTOR);
            return;
        }
        if (type == "event") {
            BroadcastController.callback(dataRep.get("element").as(Event.class), VertxUtilities.INSTANCE_AS_EXECUTOR);
            return;
        }
        if (type == "multipleCallback") {
            BroadcastController.callback(dataRep.get("element").as(MultipleCallback.class), VertxUtilities.INSTANCE_AS_EXECUTOR);
            return;
        }
    }

    public static final CompletableFuture<String> connected() {
        if (NODE_ID == null) {
            connections++;
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<String> completableFuture = new CompletableFuture<>();
        INSTANCE.thenAcceptAsync(vt -> vt.runOnContext(h -> {
            String result = CLUSTERS.entrySet().stream().filter(it -> it.getValue() < connections).map(it -> it.getKey()).findFirst().orElse(null);
            if (result == null) {
                publish("connections", ++connections);
            }
            completableFuture.complete(result == null ? null : result.split("_")[1]);
        }));
        return completableFuture;
    }

    public static final void disconnected() {
        publish("connections", --connections);
    }

    public static final void publish(String topic, Object o) {
        if (NODE_ID == null) {
            return;
        }
        INSTANCE.thenAcceptAsync(it -> it.runOnContext(h -> it.eventBus().publish("message", new ClusterMessage(NODE_ID, topic, o).toJson())));
    }

    public static final void publish(String topic) {
        publish(topic, null);
    }
}