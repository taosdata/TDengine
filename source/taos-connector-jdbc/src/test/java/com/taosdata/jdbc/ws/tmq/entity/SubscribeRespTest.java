package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.utils.JsonUtil;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class SubscribeRespTest {

    @Test
    public void testListInstancesDeserializesFromResponseField() throws Exception {
        String json = "{\"code\":0,\"message\":\"\",\"list_instances\":[\"host1:6041\",\"host2:6041\"]}";

        SubscribeResp resp = JsonUtil.getObjectMapper().readValue(json, SubscribeResp.class);

        assertArrayEquals(new String[]{"host1:6041", "host2:6041"}, resp.getListInstances());
    }

}
