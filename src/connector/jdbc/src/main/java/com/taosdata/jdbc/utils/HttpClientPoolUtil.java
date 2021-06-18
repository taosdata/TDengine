package com.taosdata.jdbc.utils;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;


public class HttpClientPoolUtil {

    private static final String DEFAULT_CONTENT_TYPE = "application/json";
    private static final int DEFAULT_TIME_OUT = 15000;
    private static final int DEFAULT_MAX_PER_ROUTE = 32;
    private static final int DEFAULT_MAX_TOTAL = 1000;
    private static final int DEFAULT_HTTP_KEEP_TIME = 15000;

    private static PoolingHttpClientConnectionManager connectionManager;
    private static CloseableHttpClient httpClient;

    private static synchronized void initPools() {
        if (httpClient == null) {
            connectionManager = new PoolingHttpClientConnectionManager();
            connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
            connectionManager.setMaxTotal(DEFAULT_MAX_TOTAL);
            httpClient = HttpClients.custom().setKeepAliveStrategy(DEFAULT_KEEP_ALIVE_STRATEGY).setConnectionManager(connectionManager).build();
        }
    }

    private static ConnectionKeepAliveStrategy DEFAULT_KEEP_ALIVE_STRATEGY = (response, context) -> {
        HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
        int keepTime = DEFAULT_HTTP_KEEP_TIME * 1000;
        while (it.hasNext()) {
            HeaderElement headerElement = it.nextElement();
            String param = headerElement.getName();
            String value = headerElement.getValue();
            if (value != null && param.equalsIgnoreCase("timeout")) {
                try {
                    return Long.parseLong(value) * 1000;
                } catch (Exception e) {
                    new Exception("format KeepAlive timeout exception, exception:" + e.toString()).printStackTrace();
                }
            }
        }
        return keepTime;
    };

    /**
     * 执行http post请求
     * 默认采用Content-Type：application/json，Accept：application/json
     *
     * @param uri  请求地址
     * @param data 请求数据
     * @return responseBody
     */
    public static String execute(String uri, String data, String token) {
        long startTime = System.currentTimeMillis();
        HttpEntity httpEntity = null;
        HttpEntityEnclosingRequestBase method = null;
        String responseBody = "";
        try {
            if (httpClient == null) {
                initPools();
            }
            method = (HttpEntityEnclosingRequestBase) getRequest(uri, HttpPost.METHOD_NAME, DEFAULT_CONTENT_TYPE, 0);
            method.setHeader("Content-Type", "text/plain");
            method.setHeader("Connection", "keep-alive");
            method.setHeader("Authorization", "Taosd " + token);

            method.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
            HttpContext context = HttpClientContext.create();
            CloseableHttpResponse httpResponse = httpClient.execute(method, context);
            httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                responseBody = EntityUtils.toString(httpEntity, "UTF-8");
            }
        } catch (Exception e) {
            if (method != null) {
                method.abort();
            }
            new Exception("execute post request exception, url:" + uri + ", exception:" + e.toString() + ", cost time(ms):" + (System.currentTimeMillis() - startTime)).printStackTrace();
        } finally {
            if (httpEntity != null) {
                try {
                    EntityUtils.consumeQuietly(httpEntity);
                } catch (Exception e) {
                    new Exception("close response exception, url:" + uri + ", exception:" + e.toString() + ", cost time(ms):" + (System.currentTimeMillis() - startTime)).printStackTrace();
                }
            }
        }
        return responseBody;
    }

    /**
     * * 创建请求
     *
     * @param uri         请求url
     * @param methodName  请求的方法类型
     * @param contentType contentType类型
     * @param timeout     超时时间
     * @return HttpRequestBase    返回类型
     * @author lisc
     */
    private static HttpRequestBase getRequest(String uri, String methodName, String contentType, int timeout) {
        if (httpClient == null) {
            initPools();
        }
        HttpRequestBase method;
        if (timeout <= 0) {
            timeout = DEFAULT_TIME_OUT;
        }
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeout * 1000)
                .setConnectTimeout(timeout * 1000).setConnectionRequestTimeout(timeout * 1000)
                .setExpectContinueEnabled(false).build();
        if (HttpPut.METHOD_NAME.equalsIgnoreCase(methodName)) {
            method = new HttpPut(uri);
        } else if (HttpPost.METHOD_NAME.equalsIgnoreCase(methodName)) {
            method = new HttpPost(uri);
        } else if (HttpGet.METHOD_NAME.equalsIgnoreCase(methodName)) {
            method = new HttpGet(uri);
        } else {
            method = new HttpPost(uri);
        }

        if (contentType == null || contentType.isEmpty() || contentType.replaceAll("\\s", "").isEmpty()) {
            contentType = DEFAULT_CONTENT_TYPE;
        }
        method.addHeader("Content-Type", contentType);
        method.addHeader("Accept", contentType);
        method.setConfig(requestConfig);
        return method;
    }

    /**
     * 执行GET 请求
     *
     * @param uri 网址
     * @return responseBody
     */
    public static String execute(String uri) {
        long startTime = System.currentTimeMillis();
        HttpEntity httpEntity = null;
        HttpRequestBase method = null;
        String responseBody = "";
        try {
            if (httpClient == null) {
                initPools();
            }
            method = getRequest(uri, HttpGet.METHOD_NAME, DEFAULT_CONTENT_TYPE, 0);
            HttpContext context = HttpClientContext.create();
            CloseableHttpResponse httpResponse = httpClient.execute(method, context);
            httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                responseBody = EntityUtils.toString(httpEntity, "UTF-8");
            }
        } catch (Exception e) {
            if (method != null) {
                method.abort();
            }
            e.printStackTrace();
        } finally {
            if (httpEntity != null) {
                try {
                    EntityUtils.consumeQuietly(httpEntity);
                } catch (Exception e) {
                    new Exception("close response exception, url:" + uri + ", exception:" + e.toString() + ",cost time(ms):" + (System.currentTimeMillis() - startTime)).printStackTrace();
                }
            }
        }
        return responseBody;
    }
}