package com.metaring.framework.ext.rpc.vertx;

import com.metaring.framework.Tools;
import com.metaring.framework.type.DataRepresentation;

public class ClusterMessage {

    public final String ID;
    public final String topic;
    public final DataRepresentation data;

    public ClusterMessage(String ID, String topic, Object o) {
        this.ID = ID;
        this.topic = topic;
        this.data = o == null ? null : o instanceof DataRepresentation ? (DataRepresentation) o : Tools.FACTORY_DATA_REPRESENTATION.fromObject(o);
    }

    public final String toJson() {
        return Tools.FACTORY_DATA_REPRESENTATION.create().add("ID", ID).add("topic", topic).add("data", data).toJson();
    }

    public static final ClusterMessage fromJson(String json) {
        DataRepresentation dataRep = Tools.FACTORY_DATA_REPRESENTATION.fromJson(json);
        return new ClusterMessage(dataRep.getText("ID"), dataRep.getText("topic"), dataRep.get("data"));
    }

}
