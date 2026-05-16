package com.taosdata.taosx.pspace;

import org.junit.Test;

import com.google.gson.Gson;

import static org.junit.Assert.*;

public class NodeTest {

    @Test
    public void testJson() {
        Node node = new Node();

        node.setId(1L);
        node.setName("望京");
        node.setLongName("\\北京\\朝阳\\望京");
        node.setIsLeaf(true);

        Gson gson = new Gson();
        String json = gson.toJson(node);
        System.out.println(json);
        // In JSON, backslashes must be escaped as \\\\
        String expectedJson = "{\"id\":1,\"name\":\"望京\",\"long_name\":\"\\\\北京\\\\朝阳\\\\望京\",\"is_leaf\":true}";
        assertEquals(expectedJson, json);
    }
}
