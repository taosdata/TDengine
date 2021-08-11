package com.taosdata.tsync;

import com.taosdata.tsync.utils.CommandLineUtil;
import com.taosdata.tsync.utils.DataGenerator;
import com.taosdata.tsync.utils.Utils;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SocketToolClient {
    private static int DEFAULT_THREAD = 1;
    private static int DEFAULT_TOTAL = 1;
    private static int DEFAULT_MESSAGE_LENGTH = 996;
    private static String help = "Usage: java -jar socket-client.jar --server [server] --port [port] --thread [thread] --total [total] --message-length [length]";
    private static final Random random = new Random(System.currentTimeMillis());

    private String server;
    private int port;
    private int thread;
    private int total;
    private int messageLength;

    public static void main(String[] args) {
        String[] configNames = new String[]{"server", "port", "thread", "total", "message-length"};
        Map<String, String> configuration = CommandLineUtil.readCommandLine(args, configNames);
        if (!configuration.containsKey("server") || !configuration.containsKey("port")) {
            System.out.println(help);
            System.exit(0);
        }
        SocketToolClient client = new SocketToolClient();
        client.server = configuration.get("server");
        client.port = Integer.parseInt(configuration.get("port"));
        client.thread = configuration.containsKey("thread") ? Integer.parseInt(configuration.get("thread")) : DEFAULT_THREAD;
        client.total = configuration.containsKey("total") ? Integer.parseInt(configuration.get("total")) : DEFAULT_TOTAL;
        client.messageLength = configuration.containsKey("message-length") ? Integer.parseInt(configuration.get("message-length")) : DEFAULT_MESSAGE_LENGTH;

        client.sendMessage();
    }

    private void sendMessage() {
        List<Long> groups = Utils.divideIntoGroups(total, thread);

        groups.forEach(messageSize -> new Thread(() -> {
            try {
                Socket socket = new Socket(server, port);
                try (PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(socket.getOutputStream()))))) {
                    for (int i = 0; i < messageSize; i++) {
                        writer.println(DataGenerator.randomString(messageLength));
                        writer.flush();
                    }
                }
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start());
    }

}
