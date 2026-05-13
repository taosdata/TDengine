package com.taosdata.jdbc.cloud;

import com.taosdata.jdbc.tmq.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Ignore
public class ConsumerTest {
    String url = null;

    @Before
    public void before() throws SQLException {
        url = System.getenv("TDENGINE_CLOUD_URL");
        if (url == null || "".equals(url.trim())) {
            System.out.println("Environment variable for CloudTest not set properly");
            return;
        }

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("insert into javatest.tmq values(now, 1, 1.1, '中国','一', true)" +
                    "(now+1s, 1, 1.1, '中国', '二', true)" +
                    "(now+2s, 1, 1.1, '中国', '三', true)");
        }
    }

    @Test
    public void testWSBeanObject() throws Exception {
        if (url == null || "".equals(url.trim())) {
            return;
        }
        // create topic sql
        String topic = "topic_java_ws_bean";

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_URL, url);
        properties.setProperty(TMQConstants.CONNECT_TIMEOUT, "10000");
        properties.setProperty(TMQConstants.CONNECT_MESSAGE_TIMEOUT, "10000");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.cloud.BeanDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");

        try (TaosConsumer<Bean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Bean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Bean> r : consumerRecords) {
                    Bean bean = r.value();
                    Assert.assertEquals(1, bean.getC1());
                    Assert.assertEquals(1.1, bean.getC2(), 0.000001);
                    Assert.assertEquals("中国", bean.getC3());
                    Assert.assertTrue(bean.isC5());
                }
            }
            consumer.unsubscribe();
        }
    }
}

class BeanDeserializer extends ReferenceDeserializer<Bean> {

}

class Bean {
    private Timestamp ts;
    private int c1;
    private Float c2;
    private String c3;
    private byte[] c4;
    private boolean c5;

    public Bean() {
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public int getC1() {
        return c1;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public Float getC2() {
        return c2;
    }

    public void setC2(Float c2) {
        this.c2 = c2;
    }

    public String getC3() {
        return c3;
    }

    public void setC3(String c3) {
        this.c3 = c3;
    }

    public byte[] getC4() {
        return c4;
    }

    public void setC4(byte[] c4) {
        this.c4 = c4;
    }

    public boolean isC5() {
        return c5;
    }

    public void setC5(boolean c5) {
        this.c5 = c5;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Bean{");
        sb.append("ts=").append(ts);
        sb.append(", c1=").append(c1);
        sb.append(", c2=").append(c2);
        sb.append(", c3='").append(c3).append('\'');
        sb.append(", c4=");
        if (c4 == null) sb.append("null");
        else {
            sb.append("'").append(new String(c4)).append("'");
        }
        sb.append(", c5=").append(c5);
        sb.append('}');
        return sb.toString();
    }
}
