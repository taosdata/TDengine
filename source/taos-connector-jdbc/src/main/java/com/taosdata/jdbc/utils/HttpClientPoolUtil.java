package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

public class HttpClientPoolUtil {

    private HttpClientPoolUtil(){}

    private static final String DEFAULT_CONTENT_TYPE = "application/json";
    private static final String DEFAULT_ACCEPT_ENCODING = "gzip, deflate";
    private static final int DEFAULT_MAX_RETRY_COUNT = 5;

    public static final String DEFAULT_HTTP_KEEP_ALIVE = "true";
    public static final String DEFAULT_MAX_PER_ROUTE = "20";
    private static final int DEFAULT_HTTP_KEEP_TIME = -1;
    public static final String DEFAULT_CONNECT_TIMEOUT = "60000";
    public static final String DEFAULT_SOCKET_TIMEOUT = "60000";
    private static String isKeepAlive;

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

    @SuppressWarnings("java:S3077")
    private static volatile CloseableHttpClient httpClient;
    private static int connectTimeout = 0;
    private static int socketTimeout = 0;
    private static boolean enableCompress = false;

    public static void init(Properties props) {
        int poolSize = Integer.parseInt(props.getProperty(TSDBDriver.HTTP_POOL_SIZE, HttpClientPoolUtil.DEFAULT_MAX_PER_ROUTE));
        boolean keepAlive = Boolean.parseBoolean(props.getProperty(TSDBDriver.HTTP_KEEP_ALIVE, HttpClientPoolUtil.DEFAULT_HTTP_KEEP_ALIVE));
        connectTimeout = Integer.parseInt(props.getProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, HttpClientPoolUtil.DEFAULT_CONNECT_TIMEOUT));
        socketTimeout = Integer.parseInt(props.getProperty(TSDBDriver.HTTP_SOCKET_TIMEOUT, HttpClientPoolUtil.DEFAULT_SOCKET_TIMEOUT));
        enableCompress = Boolean.parseBoolean(props.getProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "false"));
        if (httpClient == null) {
            synchronized (HttpClientPoolUtil.class) {
                if (httpClient == null) {
                    isKeepAlive = keepAlive ? HTTP.CONN_KEEP_ALIVE : HTTP.CONN_CLOSE;
                    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
                    connectionManager.setMaxTotal(poolSize * 10);
                    connectionManager.setDefaultMaxPerRoute(poolSize);
                    httpClient = HttpClients.custom()
                            .setKeepAliveStrategy(DEFAULT_KEEP_ALIVE_STRATEGY)
                            .setConnectionManager(connectionManager)
                            .setRetryHandler((exception, executionCount, httpContext) -> executionCount < DEFAULT_MAX_RETRY_COUNT)
                            .build();
                }
            }
        }
    }

    public static String execute(String uri, String data, String auth) throws SQLException {
        return execute(uri, data, auth, null);
    }

    /*** execute POST request ***/
    public static String execute(String uri, String data, String auth, Long reqId) throws SQLException {

        if (reqId != null) {
            if (uri.contains("?"))
                uri = uri + "&reqId=" + reqId;
            else
                uri = uri + "?reqId=" + reqId;
        }

        HttpEntityEnclosingRequestBase method = (HttpEntityEnclosingRequestBase) getRequest(uri, HttpPost.METHOD_NAME);
        method.setHeader(HTTP.CONTENT_TYPE, "text/plain");
        method.setHeader(HTTP.CONN_DIRECTIVE, isKeepAlive);
        if (auth != null) {
            method.setHeader("Authorization", auth);
        }

        if (enableCompress){
            method.addHeader("Content-Encoding", "gzip");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gos;
            try {
                gos = new GZIPOutputStream(baos);
                gos.write(data.getBytes(StandardCharsets.UTF_8));
                gos.close();
            } catch (IOException e) {
                throw new SQLException("gzip io error");
            }

            byte[] compressedData = baos.toByteArray();
            // 创建带有压缩数据的 HttpEntity
            HttpEntity entity = new ByteArrayEntity(compressedData);
            method.setEntity(entity);
        } else {
            method.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
        }

        HttpContext context = HttpClientContext.create();

        HttpEntity httpEntity = null;
        String responseBody = null;
        try (CloseableHttpResponse httpResponse = httpClient.execute(method, context)) {
            // Buffer response content
            httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                responseBody = EntityUtils.toString(httpEntity, StandardCharsets.UTF_8);
            }

            int status = httpResponse.getStatusLine().getStatusCode();
            switch (status) {
                case HttpStatus.SC_BAD_REQUEST:
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION,
                            responseBody != null && !responseBody.isEmpty() ? responseBody : String.format("http status code: %d, parameter error!", status));
                case HttpStatus.SC_UNAUTHORIZED:
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION,
                            responseBody != null && !responseBody.isEmpty() ? responseBody : String.format("http status code: %d, authorization error!", status));
                case HttpStatus.SC_FORBIDDEN:
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION,
                            responseBody != null && !responseBody.isEmpty() ? responseBody : String.format("http status code: %d, access forbidden!", status));
                case HttpStatus.SC_NOT_FOUND:
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION,
                            responseBody != null && !responseBody.isEmpty() ? responseBody : String.format("http status code: %d, url does not found!", status));
                case HttpStatus.SC_NOT_ACCEPTABLE:
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION,
                            responseBody != null && !responseBody.isEmpty() ? responseBody : String.format("http status code: %d, not acceptable!", status));
                case HttpStatus.SC_INTERNAL_SERVER_ERROR:
                case HttpStatus.SC_BAD_GATEWAY:
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION,
                            responseBody != null && !responseBody.isEmpty() ? responseBody : String.format("http status code: %d, server error!", status));
                case HttpStatus.SC_SERVICE_UNAVAILABLE:
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION,
                            responseBody != null && !responseBody.isEmpty() ? responseBody : String.format("http status code: %d, service unavailable!", status));
                default: // 2**
                    if (httpEntity == null) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_HTTP_ENTITY_IS_NULL, String.format("httpEntity is null, sql: %s, http status code: %d", data, status));
                    }
                    if (responseBody == null || responseBody.isEmpty()) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, String.format("sql: %s, http status code: %d", data, status));
                    }
            }
        } catch (ClientProtocolException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_PROTOCOL_EXCEPTION, e.getMessage());
        } catch (IOException exception) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION, exception.getMessage());
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
                .setExpectContinueEnabled(false)
                .setConnectTimeout(connectTimeout)
                .setConnectionRequestTimeout(1000)
                .setSocketTimeout(socketTimeout)
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
        method.addHeader("Accept-Encoding", DEFAULT_ACCEPT_ENCODING);

        method.setConfig(requestConfig);
        return method;
    }
}