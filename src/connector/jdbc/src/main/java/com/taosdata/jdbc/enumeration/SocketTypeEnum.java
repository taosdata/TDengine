package com.taosdata.jdbc.enumeration;

/**
 * the socket type used by tdengine
 */
public enum SocketTypeEnum {


    UDP("udp", "1"),


    TCP("tcp", "0");

    private String code;

    private String value;


    SocketTypeEnum(String code, String value){
        this.code = code;
        this.value = value;
    }

    public String getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }

    public static SocketTypeEnum getSockeTypeEnumByCode(String code){
        if (code == null || "".equals(code)){
            return null;
        }

        code = code.toLowerCase();

        for (SocketTypeEnum socketTypeEnum : SocketTypeEnum.values()){
            if (code.equals(socketTypeEnum.getCode())){
                return socketTypeEnum;
            }
        }

        return null;
    }

}
