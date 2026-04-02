package com.taosdata.tsync;

import com.taosdata.tsync.utils.CommandLineUtil;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SocketToolServer {

    private static String help = "Usage: java -jar socket-server.jar --port [port]";

    public static void main(String[] args) throws IOException {
        String[] configNames = new String[]{"port"};

        Map<String, String> configuration = CommandLineUtil.readCommandLine(args, configNames);
        if (!configuration.containsKey("port")) {
            System.out.println(help);
            System.exit(0);
        }

        int port = Integer.parseInt(configuration.get("port"));
        ServerSocket server = new ServerSocket(port);
        System.out.println("socket server started.");
        while (true) {
            Socket socket = server.accept();
            new Thread(new Task(socket)).start();
        }
    }

    private static class Task implements Runnable {
        private static final AtomicLong count = new AtomicLong(0);
        private final Socket socket;

        private Task(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                SocketAddress remote = socket.getRemoteSocketAddress();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(socket.getInputStream())))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println("receive from " + remote + "[" + count.incrementAndGet() + "] >>> " + line);
                    }
                }
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
