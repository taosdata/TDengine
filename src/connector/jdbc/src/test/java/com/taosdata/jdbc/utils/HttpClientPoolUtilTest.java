package com.taosdata.jdbc.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HttpClientPoolUtilTest {

    String user = "root";
    String password = "taosdata";
    String host = "127.0.0.1";

    @Test
    public void useLog() {
        // given
        int multi = 10;

        // when
        List<Thread> threads = IntStream.range(0, multi).mapToObj(i -> new Thread(() -> {
            try {
                String token = login(multi);
                executeOneSql("use log", token);
            } catch (SQLException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        })).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void tableNotExist() {
        // given
        int multi = 20;

        // when
        List<Thread> threads = IntStream.range(0, multi * 25).mapToObj(i -> new Thread(() -> {
            try {
//                String token = "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04";
                String token = login(multi);
                executeOneSql("insert into log.tb_not_exist values(now, 1)", token);
                executeOneSql("select last(*) from log.dn", token);
            } catch (SQLException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        })).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String login(int connPoolSize) throws SQLException, UnsupportedEncodingException {
        user = URLEncoder.encode(user, StandardCharsets.UTF_8.displayName());
        password = URLEncoder.encode(password, StandardCharsets.UTF_8.displayName());
        String loginUrl = "http://" + host + ":" + 6041 + "/rest/login/" + user + "/" + password + "";
        HttpClientPoolUtil.init(connPoolSize, false);
        String result = HttpClientPoolUtil.execute(loginUrl);
        JSONObject jsonResult = JSON.parseObject(result);
        String status = jsonResult.getString("status");
        String token = jsonResult.getString("desc");
        if (!status.equals("succ")) {
            throw new SQLException(jsonResult.getString("desc"));
        }
        return "Basic " + token;
    }

    private boolean executeOneSql(String sql, String token) throws SQLException {
        String url = "http://" + host + ":6041/rest/sql";
        String result = HttpClientPoolUtil.execute(url, sql, token);
        JSONObject resultJson = JSON.parseObject(result);
        if (resultJson.getString("status").equals("error")) {
//            HttpClientPoolUtil.reset();
//            throw TSDBError.createSQLException(resultJson.getInteger("code"), resultJson.getString("desc"));
            return false;
        }
        return true;
    }


}