package com.taosdata.netty.model.enums;

/**
 * 消息类型枚举类
 *
 * @author ZYP
 */
public enum MessageTypeEnums {

    PING(1),
    PONG(2),
    MSG_REQ(3),
    MSG_RES(4);

    /**
     * 消息类型
     */
    private byte value;

    MessageTypeEnums(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return this.value;
    }
}
