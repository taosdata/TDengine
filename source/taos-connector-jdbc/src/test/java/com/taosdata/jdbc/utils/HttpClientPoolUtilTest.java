package com.taosdata.jdbc.utils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.taosdata.jdbc.TSDBDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class HttpClientPoolUtilTest {

    private HttpServer httpServer;
    private int serverPort;
    private AtomicInteger requestCount;
    private AtomicReference<String> lastRequestBody;
    private AtomicReference<String> lastRequestAuthHeader;

    @Before
    public void setUp() throws IOException {
        // Find an available port
        serverPort = findAvailablePort();
        httpServer = HttpServer.create(new InetSocketAddress("localhost", serverPort), 0);
        requestCount = new AtomicInteger(0);
        lastRequestBody = new AtomicReference<>();
        lastRequestAuthHeader = new AtomicReference<>();

        resetStaticFields();
    }

    @After
    public void tearDown() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
        resetStaticFields();
    }

    private int findAvailablePort() throws IOException {
        // Return the available port assigned by the system
        // The ServerSocket is closed before returning, freeing the port for later use
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            // Port 0 means let the system assign an available port
            int port = socket.getLocalPort();
            // The HttpServer will later bind to this port in setUp()
            return port;
        }
    }

    private void resetStaticFields() {
        try {
            Field httpClientField = HttpClientPoolUtil.class.getDeclaredField("httpClient");
            httpClientField.setAccessible(true);
            httpClientField.set(null, null);

            Field isKeepAliveField = HttpClientPoolUtil.class.getDeclaredField("isKeepAlive");
            isKeepAliveField.setAccessible(true);
            isKeepAliveField.set(null, null);

            Field connectTimeoutField = HttpClientPoolUtil.class.getDeclaredField("connectTimeout");
            connectTimeoutField.setAccessible(true);
            connectTimeoutField.set(null, 0);

            Field socketTimeoutField = HttpClientPoolUtil.class.getDeclaredField("socketTimeout");
            socketTimeoutField.setAccessible(true);
            socketTimeoutField.set(null, 0);

            Field enableCompressField = HttpClientPoolUtil.class.getDeclaredField("enableCompress");
            enableCompressField.setAccessible(true);
            enableCompressField.set(null, false);

        } catch (Exception e) {
            // Ignore reflection issues
        }
    }

    private void initHttpClientPoolUtil(int connectTimeout, int socketTimeout) {
        Properties props = new Properties();
        props.setProperty(TSDBDriver.HTTP_POOL_SIZE, "10");
        props.setProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, String.valueOf(connectTimeout));
        props.setProperty(TSDBDriver.HTTP_SOCKET_TIMEOUT, String.valueOf(socketTimeout));
        props.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "false");
        HttpClientPoolUtil.init(props);
    }

    @Test
    public void testExecute_SuccessfulPostRequest_ReturnsResponseBody() throws Exception {
        // Arrange
        String endpoint = "/test";
        String expectedResponse = "{\"status\":\"success\"}";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                // Read request body
                byte[] requestBytes = readAllBytes(exchange.getRequestBody());
                lastRequestBody.set(new String(requestBytes, StandardCharsets.UTF_8));

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                byte[] response = expectedResponse.getBytes();
                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act
        String result = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test data",
                null
        );

        // Assert
        assertEquals(expectedResponse, result);
        assertEquals(1, requestCount.get());
        assertEquals("test data", lastRequestBody.get());
    }

    @Test
    public void testExecute_WithAuthorizationHeader_IncludesAuthInRequest() throws Exception {
        // Arrange
        String endpoint = "/auth";
        String authToken = "Bearer test-token-123";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                lastRequestAuthHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                String response = "{\"auth\":\"received\"}";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act
        String result = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test data",
                authToken
        );

        // Assert
        assertEquals("{\"auth\":\"received\"}", result);
        assertEquals(authToken, lastRequestAuthHeader.get());
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http400BadRequest_ThrowsSQLException() throws Exception {
        testErrorStatusCode(400, "Bad Request");
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http401Unauthorized_ThrowsSQLException() throws Exception {
        testErrorStatusCode(401, "Unauthorized");
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http403Forbidden_ThrowsSQLException() throws Exception {
        testErrorStatusCode(403, "Forbidden");
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http404NotFound_ThrowsSQLException() throws Exception {
        testErrorStatusCode(404, "Not Found");
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http406NotAcceptable_ThrowsSQLException() throws Exception {
        testErrorStatusCode(406, "Not Acceptable");
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http500InternalServerError_ThrowsSQLException() throws Exception {
        testErrorStatusCode(500, "Internal Server Error");
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http502BadGateway_ThrowsSQLException() throws Exception {
        testErrorStatusCode(502, "Bad Gateway");
    }

    @Test(expected = SQLException.class)
    public void testExecute_Http503ServiceUnavailable_ThrowsSQLException() throws Exception {
        testErrorStatusCode(503, "Service Unavailable");
    }

    private void testErrorStatusCode(int statusCode, String statusText) throws Exception {
        // Arrange
        String endpoint = "/error";
        String errorMessage = "Test error: " + statusCode;

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                exchange.sendResponseHeaders(statusCode, errorMessage.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorMessage.getBytes());
                }
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act & Assert
        try {
            HttpClientPoolUtil.execute(
                    "http://localhost:" + serverPort + endpoint,
                    "test data",
                    null
            );
        } catch (SQLException e) {
            // Verify error message contains status code or error message
            assertTrue("Exception message should contain status code or error",
                    e.getMessage().contains(String.valueOf(statusCode)) ||
                            e.getMessage().contains(errorMessage));
            throw e;
        }
    }

    @Test
    public void testExecute_EmptyResponseBodyWith200Status_ThrowsSQLException() {
        // Arrange
        String endpoint = "/empty";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, -1); // No response body
                exchange.getResponseBody().close();
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act & Assert
        try {
            HttpClientPoolUtil.execute(
                    "http://localhost:" + serverPort + endpoint,
                    "test data",
                    null
            );
            fail("Expected SQLException for empty response body");
        } catch (SQLException e) {
            assertTrue("Exception should mention empty response",
                    e.getMessage().contains("http status code") ||
                            e.getMessage().contains("empty"));
        }
    }

    @Test
    public void testExecute_NullResponseEntityWith200Status_ThrowsSQLException() {
        // Arrange
        String endpoint = "/null-entity";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                // Send response without entity
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().close();
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act & Assert
        try {
            HttpClientPoolUtil.execute(
                    "http://localhost:" + serverPort + endpoint,
                    "test data",
                    null
            );
            fail("Expected SQLException for null response entity");
        } catch (SQLException e) {
            assertTrue("Exception should mention status code: 200",
                    e.getMessage().contains("ttp status code: 200"));
        }
    }

    @Test
    public void testExecute_WithRequestId_AppendsReqIdParameter() throws Exception {
        // Arrange
        String endpoint = "/reqid";
        Long reqId = 12345L;
        final String[] capturedUri = new String[1];

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                capturedUri[0] = exchange.getRequestURI().toString();

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                String response = "{\"reqId\":\"received\"}";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act
        String result = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test data",
                null,
                reqId
        );

        // Assert
        assertEquals("{\"reqId\":\"received\"}", result);
        assertTrue("URI should contain reqId parameter",
                capturedUri[0].contains("reqId=" + reqId));
    }

    @Test
    public void testExecute_WithExistingQueryParamsAndRequestId_AppendsCorrectly() throws Exception {
        // Arrange
        String endpoint = "/test?param=value";
        Long reqId = 12345L;
        final String[] capturedUri = new String[1];

        httpServer.createContext("/test", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                capturedUri[0] = exchange.getRequestURI().toString();

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                String response = "{\"status\":\"ok\"}";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act
        String result = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test data",
                null,
                reqId
        );

        // Assert
        assertEquals("{\"status\":\"ok\"}", result);
        assertTrue("URI should contain both param and reqId",
                capturedUri[0].contains("param=value") &&
                        capturedUri[0].contains("reqId=" + reqId));
    }

    @Test(expected = SQLException.class)
    public void testExecute_ConnectionTimeout_ThrowsSQLException() throws Exception {
        // Arrange
        String endpoint = "/timeout";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                // Simulate delay longer than timeout
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                String response = "{\"status\":\"delayed\"}";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        httpServer.start();

        // Set very short timeout
        initHttpClientPoolUtil(1000, 1000);

        // Act & Assert
        HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test data",
                null
        );
    }

    @Test
    public void testExecute_InvalidHost_ThrowsSQLException() {
        // Arrange
        initHttpClientPoolUtil(2000, 2000);

        // Act & Assert
        try {
            HttpClientPoolUtil.execute(
                    "http://invalid-host-that-does-not-exist:9999/test",
                    "test data",
                    null
            );
            fail("Expected SQLException for invalid host");
        } catch (SQLException e) {
            // Expected - connection should fail
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testExecute_InvalidOrUnreachableUrl_ThrowsSQLException() {
        // Arrange
        initHttpClientPoolUtil(5000, 5000);

        // Act & Assert
        try {
            HttpClientPoolUtil.execute(
                    "http://non-existent-host-that-will-never-resolve.xyz:9999/some/path",
                    "test data",
                    null
            );
            fail("Expected SQLException for unreachable host");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testExecute_ServerClosesConnectionImmediately_ThrowsSQLException() throws Exception {
        // Arrange
        String endpoint = "/close";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                // Close connection without sending response
                exchange.close();
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act & Assert
        try {
            HttpClientPoolUtil.execute(
                    "http://localhost:" + serverPort + endpoint,
                    "test data",
                    null
            );
            fail("Expected SQLException for closed connection");
        } catch (SQLException e) {
            // Expected
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testExecute_LargeResponseBody_HandlesCorrectly() throws Exception {
        // Arrange
        String endpoint = "/large";
        StringBuilder largeResponse = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeResponse.append("Line ").append(i).append(": This is a test response\n");
        }
        String expectedResponse = largeResponse.toString();

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                exchange.sendResponseHeaders(200, expectedResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(expectedResponse.getBytes());
                }
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(10000, 10000);

        // Act
        String result = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test data",
                null
        );

        // Assert
        assertEquals(expectedResponse, result);
    }

    @Test
    public void testInit_SingletonPattern_InitializesOnlyOnce() throws Exception {
        // Arrange
        String endpoint = "/singleton";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                String response = "{\"status\":\"ok\"}";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        httpServer.start();

        // First initialization
        Properties props1 = new Properties();
        props1.setProperty(TSDBDriver.HTTP_POOL_SIZE, "5");
        props1.setProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, "1000");
        props1.setProperty(TSDBDriver.HTTP_SOCKET_TIMEOUT, "1000");
        HttpClientPoolUtil.init(props1);

        // Second initialization with different values
        Properties props2 = new Properties();
        props2.setProperty(TSDBDriver.HTTP_POOL_SIZE, "20");
        props2.setProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, "5000");
        props2.setProperty(TSDBDriver.HTTP_SOCKET_TIMEOUT, "5000");
        HttpClientPoolUtil.init(props2);

        // Act - Make request
        String result1 = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test1",
                null
        );

        // Reset static state
        resetStaticFields();

        // Initialize again
        HttpClientPoolUtil.init(props2);

        String result2 = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                "test2",
                null
        );

        // Assert
        assertEquals("{\"status\":\"ok\"}", result1);
        assertEquals("{\"status\":\"ok\"}", result2);
        assertEquals(2, requestCount.get());
    }

    @Test
    public void testExecute_SpecialCharactersInData_HandlesCorrectly() throws Exception {
        // Arrange
        String endpoint = "/special";
        String testData = "SELECT * FROM test WHERE name = 'test\"quote' AND value > 100";
        String expectedResponse = "{\"processed\":true}";

        httpServer.createContext(endpoint, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                requestCount.incrementAndGet();
                byte[] requestBytes = readAllBytes(exchange.getRequestBody());
                lastRequestBody.set(new String(requestBytes, StandardCharsets.UTF_8));

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, expectedResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(expectedResponse.getBytes());
                }
            }
        });
        httpServer.start();

        initHttpClientPoolUtil(5000, 5000);

        // Act
        String result = HttpClientPoolUtil.execute(
                "http://localhost:" + serverPort + endpoint,
                testData,
                null
        );

        // Assert
        assertEquals(expectedResponse, result);
        assertEquals(testData, lastRequestBody.get());
    }

    private byte[] readAllBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[4096]; // 4KB 缓冲区
        int bytesRead;
        while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, bytesRead);
        }
        return buffer.toByteArray();
    }
}