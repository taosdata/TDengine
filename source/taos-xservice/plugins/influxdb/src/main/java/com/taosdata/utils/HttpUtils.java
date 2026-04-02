package com.taosdata.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

/**
 * HTTP请求工具类
 *
 * @author ZYP
 */
public class HttpUtils {

    protected static Logger logger = LoggerFactory.getLogger(HttpUtils.class);


    public static Map<String, String> sendGetHeaders(String url, String param) throws Exception {
        Map<String, String> headers = new HashMap<>();
        try {
            if (StringUtils.isNotEmpty(param)) {
                url += "?" + param;
            }
            URL realUrl = new URL(url);
            URLConnection connection = realUrl.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(600000);
            connection.connect();

            // Extract headers
            for (Map.Entry<String, java.util.List<String>> entry : connection.getHeaderFields().entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    headers.put(entry.getKey(), String.join(", ", entry.getValue()));
                }
            }
        } catch (Exception e) {
            logger.error("An exception occurred while sending GET request for headers, exception={}", e.getMessage());
            throw e;
        }
        return headers;
    }

    /**
     * 发送get请求
     *
     * @param url
     * @param param
     * @return
     */
    public static String sendGet(String url, String param) throws Exception {
        StringBuffer result = new StringBuffer();
        BufferedReader in = null;
        try {
            if (StringUtils.isNotEmpty(param)) {
                url += "?" + param;
            }
            URL realUrl = new URL(url);
            URLConnection connection = realUrl.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.setConnectTimeout(5000);
            // 单次查询数据量大时会有较长等待，所以此处设置 10 分钟
            connection.setReadTimeout(600000);
            connection.connect();
            // 读取返回数据
            in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "GBK"));
            // 取第一行
            String line = in.readLine();
            // 非空则遍历读取
            while (line != null) {
                // 拼接数据
                result.append(line.trim() + "\n");
                // 继续
                line = in.readLine();
            }
        } catch (Exception e) {
            logger.error("发送get请求过程中发生异常，exception={}", e.getMessage());
            throw e;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception ex) {
                logger.error("关闭get请求输入流过程中发生异常，exception={}", ex.getMessage());
            }
        }
        return result.toString();
    }

    /**
     * 发送post json请求
     *
     * @param url
     * @param json
     * @return
     */
    /*public static String sendPostJson(String url, String json) throws Exception {
        String result = "";
        try {
            // 建立http客户端
            CloseableHttpClient httpClient = HttpClients.createDefault();
            // 建立httpPost
            HttpPost httpPost = new HttpPost(url);
            // 解决中文乱码问题
            StringEntity entity = new StringEntity(json, "utf-8");
            // 设置编码格式
            entity.setContentEncoding("UTF-8");
            // 设置参数类型为json
            entity.setContentType("application/json");
            // 添加参数
            httpPost.setEntity(entity);
            // 获取响应
            CloseableHttpResponse response = httpClient.execute(httpPost);
            // 解析结果
            if (response.getStatusLine().getStatusCode() == 200) {
                result = EntityUtils.toString(response.getEntity(), "UTF-8");
            } else {
                throw new Exception(response.getStatusLine().getReasonPhrase());
            }
        } catch (Exception e) {
            logger.error("发送post请求过程中发生异常，exception={}", e.getMessage());
            throw e;
        }
        return result;
    }*/
}
