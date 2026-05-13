package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.utils.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class TaosAdapterMock {
    private String cmd = "";
    private int downInCmdCount = 0;

    private final String targetHost;
    private final int targetPort;
    private int listenPort;
    private volatile boolean isRunning = false;
    private ServerSocket serverSocket;
    private ExecutorService executor;
    private final AtomicInteger delayMillis = new AtomicInteger(0);
    private final ConcurrentHashMap<Socket, Boolean> activeConnections = new ConcurrentHashMap<>();

    public TaosAdapterMock() {
        this.targetHost = "localhost";
        this.targetPort = 6041;
    }

    public TaosAdapterMock(String cmd, int downInCmdCount) {
        this.targetHost = "localhost";
        this.targetPort = 6041;
        this.cmd = cmd;
        this.downInCmdCount = downInCmdCount;
    }

    public TaosAdapterMock(int listenPort) {
        this.targetHost = "localhost";
        this.targetPort = 6041;
        this.listenPort = listenPort;
    }
    public int getListenPort() {
        return listenPort;
    }

    public void setDelayMillis(int delayMillis) {
        this.delayMillis.set(delayMillis);
    }

    public synchronized void start() throws IOException {
        if (isRunning) {
            throw new IllegalStateException("Service is already running");
        }

        serverSocket = new ServerSocket();
        serverSocket.bind(new java.net.InetSocketAddress(listenPort));
        if (listenPort == 0){
            listenPort = serverSocket.getLocalPort();
        }

        executor = Executors.newCachedThreadPool();
        isRunning = true;

        executor.execute(() -> {
            System.out.printf("Forward service started on port %d -> %s:%d%n",
                    listenPort, targetHost, targetPort);

            while (isRunning) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    activeConnections.put(clientSocket, true);
                    executor.execute(new ClientHandler(clientSocket, cmd, downInCmdCount));
                } catch (SocketException e) {
                    // exception in normal stop
                } catch (IOException e) {
                    if (isRunning) {
                        System.err.println("Accept failed: " + e.getMessage());
                    }
                }
            }
        });
    }

    public synchronized void stop() {
        if (!isRunning) return;
        isRunning = false;

        try {
            // close server socket
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }

        // close all active connections
        activeConnections.keySet().forEach(this::closeSocket);
        activeConnections.clear();

        // close executor service
        if (executor != null) {
            executor.shutdownNow();
        }

        System.out.println("Forward service stopped");
    }

    private class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final String cmd;
        private int downInCmdCount;

        ClientHandler(Socket clientSocket, String cmd, int downInCmdCount) {
            this.clientSocket = clientSocket;
            this.cmd = cmd;
            this.downInCmdCount = downInCmdCount;
        }

        @Override
        public void run() {
            try (Socket targetSocket = new Socket(targetHost, targetPort);
                 InputStream clientInput = clientSocket.getInputStream();
                 OutputStream clientOutput = clientSocket.getOutputStream();
                 InputStream targetInput = targetSocket.getInputStream();
                 OutputStream targetOutput = targetSocket.getOutputStream()) {

                // start two threads to forward data
                Future<?> clientToTarget = executor.submit(
                        () -> forwardData(clientInput, targetOutput, "client->target", false)
                );

                Future<?> targetToClient = executor.submit(
                        () -> forwardData(targetInput, clientOutput, "target->client", true)
                );

                // wait for both threads to finish
                clientToTarget.get();
                targetToClient.get();

            } catch (Exception e) {
                if (isRunning) {
                    System.err.println("Connection error: " + e.getMessage());
                }
            } finally {
                closeSocket(clientSocket);
                activeConnections.remove(clientSocket);
            }
        }

        private void forwardData(InputStream input, OutputStream output, String direction, boolean withDelay) {
            byte[] buffer = new byte[4096];
            int bytesRead;

            try {
                while ((bytesRead = input.read(buffer)) != -1) {
                    if (!StringUtils.isEmpty(cmd) && direction.equalsIgnoreCase("target->client") && bytesRead >= cmd.length()) {
                        String readStr = new String(buffer, 0, bytesRead);
                        if (readStr.contains(cmd)) {
                            downInCmdCount--;
                        }
                        if (downInCmdCount == 0) {
                            System.out.printf("Simulating server down after command '%s'%n", cmd);
                            stop();
                            throw new IOException("Simulated server down");
                        }
                    }

                    if (withDelay && delayMillis.get() > 0) {
                        try {
                            Thread.sleep(delayMillis.get());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    output.write(buffer, 0, bytesRead);
                    output.flush();
                }
            } catch (IOException e) {
                if (!isRunning) {
                    return; // ignore if service is stopped
                }
                System.err.printf("[%s] Forward error: %s%n", direction, e.getMessage());
            }
        }
    }

    private void closeSocket(Socket socket) {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing socket: " + e.getMessage());
        }
    }
}
    