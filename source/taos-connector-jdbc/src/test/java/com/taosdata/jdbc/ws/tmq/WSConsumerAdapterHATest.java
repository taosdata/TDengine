package com.taosdata.jdbc.ws.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.tmq.entity.ConsumerParam;
import com.taosdata.jdbc.ws.tmq.entity.SubscribeResp;
import com.taosdata.jdbc.ws.tmq.entity.TMQRequestFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Properties;

public class WSConsumerAdapterHATest {

    @Test
    public void subscribeMergesListInstancesFromSuccessfulResponse() throws Exception {
        WSConsumer<Object> consumer = new WSConsumer<>();
        CapturingTransport transport = new CapturingTransport();
        setField(consumer, "factory", new TMQRequestFactory());
        setField(consumer, "param", new ConsumerParam(properties()));
        setField(consumer, "transport", transport);

        consumer.subscribe(Collections.singleton("topic_1"));

        Assert.assertArrayEquals(new String[]{"node2:6041", "node3:6041"}, transport.getMergedInstances());
    }

    @Test
    public void subscribeSucceedsWhenAdapterHaResponseOmitsListInstancesOnOlderServer() throws Exception {
        WSConsumer<Object> consumer = new WSConsumer<>();
        CapturingTransport transport = new CapturingTransport("3.3.6.0", null);
        setField(consumer, "factory", new TMQRequestFactory());
        setField(consumer, "param", new ConsumerParam(properties()));
        setField(consumer, "transport", transport);

        consumer.subscribe(Collections.singleton("topic_1"));

        Assert.assertTrue(transport.isMergeCalled());
        Assert.assertNull(transport.getMergedInstances());
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.GROUP_ID, "group_id");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ADAPTER_HA, "true");
        return properties;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class CapturingTransport extends Transport {
        private final String version;
        private final String[] listInstances;
        private String[] mergedInstances;
        private boolean mergeCalled;

        private CapturingTransport() {
            this("3.3.6.0", new String[]{"node2:6041", "node3:6041"});
        }

        private CapturingTransport(String version, String[] listInstances) {
            this.version = version;
            this.listInstances = listInstances;
        }

        @Override
        public Response send(Request request) {
            SubscribeResp response = new SubscribeResp();
            response.setCode(Code.SUCCESS.getCode());
            response.setVersion(version);
            response.setListInstances(listInstances);
            return response;
        }

        @Override
        public void mergeDiscoveredEndpoints(String[] instances) {
            this.mergeCalled = true;
            this.mergedInstances = instances;
        }

        private String[] getMergedInstances() {
            return mergedInstances;
        }

        private boolean isMergeCalled() {
            return mergeCalled;
        }
    }
}
