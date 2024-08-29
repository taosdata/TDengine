package com.taosdata;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public class AlertConfig {

        private String host;
        private int port;
        private String protocol;
        private String topic;

    @JSONField(name = "numeric_alert")
        private List<NumericAlert> numericAlertList;
    @JSONField(name = "bool_alert")
        private List<BoolAlert> boolAlertList;
        private String alertFile;

        // Getters and Setters
        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public List<NumericAlert> getNumericAlertList() {
            return numericAlertList;
        }

        public void setNumericAlertList(List<NumericAlert> numericAlertList) {
            this.numericAlertList = numericAlertList;
        }

        public List<BoolAlert> getBoolAlertList() {
            return boolAlertList;
        }

        public void setBoolAlertList(List<BoolAlert> boolAlertList) {
            this.boolAlertList = boolAlertList;
        }

        public String getAlertFile() {
            return alertFile;
        }

        public void setAlertFile(String alertFile) {
            this.alertFile = alertFile;
        }
}