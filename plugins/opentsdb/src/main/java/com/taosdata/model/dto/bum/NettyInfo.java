package com.taosdata.model.dto.bum;

import lombok.Data;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Netty信息
 *
 * @author ZYP
 */
@Data
public class NettyInfo {

    /**
     * 服务地址
     */
    private String serverAddr;

    /**
     * 所有Socket连接
     */
    private ConcurrentHashMap<String, Connection> connectionMap;

    @Data
    public class Connection {

        /**
         * 客户端ID
         */
        private String clientId;

        /**
         * 创建时间
         */
        private Date createTime;

        /**
         * 上次通信时间
         */
        private Date activeTime;

        /**
         * 状态
         */
        private int status;

        /**
         * 描述
         */
        private String description;

        public Connection(String clientId) {
            this.clientId = clientId;
        }
    }
}
