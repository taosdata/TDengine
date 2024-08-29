package com.taosdata.taosdemo.components;

import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JsonConfig {

    public static void main(String[] args) {

        JsonConfig config = new JsonConfig();
        String str = config.read("insert.json");
        JSONObject jsonObject = JSONObject.parseObject(str);
        System.out.println(jsonObject);

    }

    private String read(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(JsonConfig.class.getClassLoader().getResourceAsStream(fileName))
            );
            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileName;
    }


}