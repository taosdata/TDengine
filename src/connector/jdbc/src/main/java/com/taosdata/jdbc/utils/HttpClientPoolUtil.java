package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
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

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class HttpClientPoolUtil {

    private static final String DEFAULT_CONTENT_TYPE = "application/json";
    private static final int DEFAULT_MAX_TOTAL = 200;
    private static final int DEFAULT_MAX_PER_ROUTE = 20;
    private static final int DEFAULT_TIME_OUT = 15000;
    private static final int DEFAULT_HTTP_KEEP_TIME = 15000;
    private static final int DEFAULT_MAX_RETRY_COUNT = 5;

    private static final ConnectionKeepAliveStrategy DEFAULT_KEEP_ALIVE_STRATEGY = (response, context) -> {
        HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
        while (it.hasNext()) {
            HeaderElement headerElement = it.nextElement();
            String param = headerElement.getName();
            String value = headerElement.getValue();
            if (value != null && param.equalsIgnoreCase("timeout")) {
                try {
                    return Long.parseLong(value) * 1000;
                } catch (NumberFormatException ignore) {
                }
            }
        }
        return DEFAULT_HTTP_KEEP_TIME * 1000;
    };

    private static final HttpRequestRetryHandler retryHandler = (exception, executionCount, httpContext) -> {
        if (executionCount >= DEFAULT_MAX_RETRY_COUNT)
            // do not retry if over max retry count
            return false;
        if (exception instanceof InterruptedIOException)
            // timeout
            return false;
        if (exception instanceof UnknownHostException)
            // unknown host
            return false;
        if (exception instanceof SSLException)
            // SSL handshake exception
            return false;
        return true;
    };

    private static CloseableHttpClient httpClient;

    static {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(DEFAULT_MAX_TOTAL);
        connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
        httpClient = HttpClients.custom().setKeepAliveStrategy(DEFAULT_KEEP_ALIVE_STRATEGY).setConnectionManager(connectionManager).setRetryHandler(retryHandler).build();
    }

    /*** execute GET request ***/
    public static String execute(String uri) throws SQLException {
        HttpEntity httpEntity = null;
        String responseBody = "";
        try {
            HttpRequestBase method = getRequest(uri, HttpGet.METHOD_NAME);
            HttpContext context = HttpClientContext.create();
            CloseableHttpResponse httpResponse = httpClient.execute(method, context);
            httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                responseBody = EntityUtils.toString(httpEntity, StandardCharsets.UTF_8);
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_Protocol_Exception, e.getMessage());
        } catch (IOException exception) {
            exception.printStackTrace();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, exception.getMessage());
        } finally {
            if (httpEntity != null) {
                EntityUtils.consumeQuietly(httpEntity);
            }
        }
        return responseBody;
    }


    /*** execute POST request ***/
    public static String execute(String uri, String data, String token) throws SQLException {
        HttpEntity httpEntity = null;
        String responseBody = "";
        try {
            HttpEntityEnclosingRequestBase method = (HttpEntityEnclosingRequestBase) getRequest(uri, HttpPost.METHOD_NAME);
            method.setHeader(HTTP.CONTENT_TYPE, "text/plain");
            method.setHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
            method.setHeader("Authorization", "Taosd " + token);

            method.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
            HttpContext context = HttpClientContext.create();
            CloseableHttpResponse httpResponse = httpClient.execute(method, context);
            httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                responseBody = EntityUtils.toString(httpEntity, StandardCharsets.UTF_8);
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_Protocol_Exception, e.getMessage());
        } catch (IOException exception) {
            exception.printStackTrace();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, exception.getMessage());
        } finally {
            if (httpEntity != null) {
                EntityUtils.consumeQuietly(httpEntity);
            }
        }
        return responseBody;
    }

    /*** create http request ***/
    private static HttpRequestBase getRequest(String uri, String methodName) {
        HttpRequestBase method;
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(DEFAULT_TIME_OUT * 1000)
                .setConnectTimeout(DEFAULT_TIME_OUT * 1000)
                .setConnectionRequestTimeout(DEFAULT_TIME_OUT * 1000)
                .setExpectContinueEnabled(false)
                .build();
        if (HttpPut.METHOD_NAME.equalsIgnoreCase(methodName)) {
            method = new HttpPut(uri);
        } else if (HttpPost.METHOD_NAME.equalsIgnoreCase(methodName)) {
            method = new HttpPost(uri);
        } else if (HttpGet.METHOD_NAME.equalsIgnoreCase(methodName)) {
            method = new HttpGet(uri);
        } else {
            method = new HttpPost(uri);
        }
        method.addHeader(HTTP.CONTENT_TYPE, DEFAULT_CONTENT_TYPE);
        method.addHeader("Accept", DEFAULT_CONTENT_TYPE);
        method.setConfig(requestConfig);
        return method;
    }

}