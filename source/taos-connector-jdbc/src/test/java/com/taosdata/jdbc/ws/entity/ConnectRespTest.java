package com.taosdata.jdbc.ws.entity;

import com.taosdata.jdbc.utils.JsonUtil;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class ConnectRespTest {

    @Test
    public void testListInstancesDeserializesFromResponseField() throws Exception {
        String json = "{\"code\":0,\"message\":\"\",\"list_instances\":[\"host1:6041\",\"host2:6041\"]}";

        ConnectResp resp = JsonUtil.getObjectMapper().readValue(json, ConnectResp.class);

        assertArrayEquals(new String[]{"host1:6041", "host2:6041"}, resp.getListInstances());
    }

}
