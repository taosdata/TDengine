package com.taosdata.jdbc.ws.tmq;

import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.common.ConsumerFactory;
import com.taosdata.jdbc.common.ConsumerManager;
import com.taosdata.jdbc.enums.ConnectionType;

public class WSConsumerFactory implements ConsumerFactory {

    static {
        ConsumerManager.register(new WSConsumerFactory());
    }

    @Override
    public boolean acceptsType(String type) {
        return ConnectionType.WS.getType().equalsIgnoreCase(type) || ConnectionType.WEBSOCKET.getType().equalsIgnoreCase(type);
    }

    @Override
    public Consumer<?> getConsumer() {
        return new WSConsumer<>();
    }
}
