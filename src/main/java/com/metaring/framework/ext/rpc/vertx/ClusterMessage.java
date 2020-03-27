package com.metaring.framework.ext.rpc.vertx;

import com.metaring.framework.Tools;
import com.metaring.framework.type.DataRepresentation;

public class ClusterMessage {
    public final String topic;
    public final DataRepresentation data;

    public ClusterMessage(String topic, Object o) {
        this.topic = topic;
        this.data = o == null ? null : o instanceof DataRepresentation ? (DataRepresentation) o : Tools.FACTORY_DATA_REPRESENTATION.fromObject(o);
    }

    public final String toJson() {
        return Tools.FACTORY_DATA_REPRESENTATION.create().add("topic", topic).add("data", data).toJson();
    }

    public static final ClusterMessage fromJson(String json) {
        DataRepresentation dataRep = Tools.FACTORY_DATA_REPRESENTATION.fromJson(json);
        return new ClusterMessage(dataRep.getText("topic"), dataRep.get("data"));
    }

}
