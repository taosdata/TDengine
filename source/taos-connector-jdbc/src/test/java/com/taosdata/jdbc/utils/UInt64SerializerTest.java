package com.taosdata.jdbc.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.ws.entity.FetchReq;
import org.junit.Assert;
import org.junit.Test;

public class UInt64SerializerTest {

    @Test
    public void write() throws JsonProcessingException {
        FetchReq fetchReq = new FetchReq();
        fetchReq.setId(-1);
        fetchReq.setReqId(-1);

        // 使用 Jackson 将对象转换为 JSON 字符串
        String s = JsonUtil.getObjectWriter().writeValueAsString(fetchReq);
        Assert.assertTrue(s.contains(TSDBConstants.MAX_UNSIGNED_LONG));

        // 使用 Jackson 将 JSON 字符串解析为对象
        FetchReq fetchReq1 = JsonUtil.getObjectReader(FetchReq.class).readValue(s);
        Assert.assertEquals(-1, fetchReq1.getId());
    }
}