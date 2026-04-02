package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.common.ConsumerFactory;
import com.taosdata.jdbc.common.ConsumerManager;
import com.taosdata.jdbc.enums.ConnectionType;

public class JNIConsumerFactory implements ConsumerFactory {

    static {
        ConsumerManager.register(new JNIConsumerFactory());
    }

    @Override
    public boolean acceptsType(String type) {
        return null == type || ConnectionType.JNI.getType().equalsIgnoreCase(type);
    }

    @Override
    public Consumer<?> getConsumer() {
        return new JNIConsumer<>();
    }
}
