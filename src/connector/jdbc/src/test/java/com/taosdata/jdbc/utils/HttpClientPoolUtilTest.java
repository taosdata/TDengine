package com.taosdata.jdbc.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HttpClientPoolUtilTest {

    String user = "root";
    String password = "taosdata";
    String host = "127.0.0.1";
    String dbname = "log";

    @Test
    public void test() {
        // given
        List<Thread> threads = IntStream.range(0, 4000).mapToObj(i -> new Thread(() -> {
            useDB();
//            try {
//                TimeUnit.SECONDS.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
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

    private void useDB() {
        try {
            user = URLEncoder.encode(user, StandardCharsets.UTF_8.displayName());
            password = URLEncoder.encode(password, StandardCharsets.UTF_8.displayName());
            String loginUrl = "http://" + host + ":" + 6041 + "/rest/login/" + user + "/" + password + "";
            String result = HttpClientPoolUtil.execute(loginUrl);
            JSONObject jsonResult = JSON.parseObject(result);
            String status = jsonResult.getString("status");
            String token = jsonResult.getString("desc");
            if (!status.equals("succ")) {
                throw new SQLException(jsonResult.getString("desc"));
            }

            String url = "http://" + host + ":6041/rest/sql";
            String sql = "use " + dbname;
            result = HttpClientPoolUtil.execute(url, sql, token);

            JSONObject resultJson = JSON.parseObject(result);
            if (resultJson.getString("status").equals("error")) {
                throw TSDBError.createSQLException(resultJson.getInteger("code"), resultJson.getString("desc"));
            }
        } catch (UnsupportedEncodingException | SQLException e) {
            e.printStackTrace();
        }
    }


}