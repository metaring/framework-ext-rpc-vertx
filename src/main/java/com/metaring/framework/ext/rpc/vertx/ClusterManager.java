package com.metaring.framework.ext.rpc.vertx;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
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
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class ClusterManager {

    private static final CompletableFuture<Vertx> INSTANCE = new CompletableFuture<>();
    private static final HazelcastClusterManager CLUSTER_MANAGER;

    public static final Map<String, Long> CLUSTERS = new HashMap<>();
    public static final List<Consumer<ClusterMessage>> CONSUMERS = new ArrayList<>();

    public static String NODE_ID;

    public static long connections = 0;

    static {
        final HazelcastClusterManager[] manager = {null};
        try {
            manager[0] = StringUtil.isNullOrEmpty(ClusterManager.class.getClassLoader().getResource("cluster.xml").toString()) ? null : new HazelcastClusterManager();
        } catch (Exception e) {
        }
        if ((CLUSTER_MANAGER = manager[0]) == null) {
            INSTANCE.complete(null);
        } else {
            Vertx.clusteredVertx(new VertxOptions().setClusterManager(CLUSTER_MANAGER), cluster -> {
                if (cluster.succeeded()) {
                    Vertx vertx = cluster.result();
                    vertx.eventBus().consumer("message", message -> handle(ClusterMessage.fromJson(message.body().toString())));
                    CLUSTER_MANAGER.getHazelcastInstance().getCluster().addMembershipListener(new MembershipListener() {
                        @Override
                        public void memberRemoved(MembershipEvent event) {
                            INSTANCE.thenAccept(it -> it.runOnContext(v -> handle(new ClusterMessage(event.getMember().getUuid() + "_" + (event.getMember().getSocketAddress().toString().replace("/", "").split(":")[0]), "unsync", null))));
                        }

                        @Override
                        public void memberAttributeChanged(MemberAttributeEvent arg0) {
                        }

                        @Override
                        public void memberAdded(MembershipEvent arg0) {
                        }
                    });
                    BroadcastController.register((type, element) -> publish("broadcast", Tools.FACTORY_DATA_REPRESENTATION.create().add("type", type).add("element", element)));
                    NODE_ID = CLUSTER_MANAGER.getNodeID() + "_" + (CLUSTER_MANAGER.getHazelcastInstance().getLocalEndpoint().getSocketAddress().toString().replace("/", "").split(":")[0]);
                    INSTANCE.complete(vertx);
                    publish("sync", connections);
                } else {
                    INSTANCE.completeExceptionally(cluster.cause());
                }
            });
        }
    }

    private static final void handle(final ClusterMessage clusterMessage) {
        if(!NODE_ID.equalsIgnoreCase(clusterMessage.ID)) {
            try {
                Method method = ClusterManager.class.getDeclaredMethod(clusterMessage.topic, String.class, DataRepresentation.class);
                method.setAccessible(true);
                method.invoke(null, clusterMessage.ID, clusterMessage.data);
            } catch (Exception e) {
            }
        }
        for(int i = 0; i < CONSUMERS.size(); i++) {
            try {
                Consumer<ClusterMessage> consumer = CONSUMERS.get(i);
                VertxUtilities.INSTANCE.runOnContext(v -> consumer.accept(clusterMessage));
            } catch(Exception e) {
            }
        }
    }

    @SuppressWarnings("unused")
    private static final void sync(String caller, DataRepresentation dataRep) {
        CLUSTERS.put(caller, dataRep.asDigit());
        publish("connections", connections);
    }

    @SuppressWarnings("unused")
    private static final void unsync(String caller, DataRepresentation dataRep) {
        CLUSTERS.remove(caller);
    }

    @SuppressWarnings("unused")
    private static final void connections(String caller, DataRepresentation dataRep) {
        CLUSTERS.put(caller, dataRep.asDigit());
    }

    @SuppressWarnings("unused")
    private static final void broadcast(String caller, DataRepresentation dataRep) {
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

    public static final boolean isMaster() {
        try {
            Member member = CLUSTER_MANAGER.getHazelcastInstance().getCluster().getMembers().iterator().next();
            return NODE_ID.equalsIgnoreCase(member.getUuid() + "_" + (member.getSocketAddress().toString().replace("/", "").split(":")[0]));
        } catch(Exception e) {
            e.printStackTrace();
            return false;
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
        INSTANCE.thenAccept(it -> it.runOnContext(h -> it.eventBus().publish("message", new ClusterMessage(NODE_ID, topic, o).toJson())));
    }

    public static final void publish(String topic) {
        publish(topic, null);
    }
}