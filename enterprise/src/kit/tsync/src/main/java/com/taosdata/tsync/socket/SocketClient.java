package com.taosdata.tsync.socket;

import java.io.*;
import java.net.Socket;

public class SocketClient {

    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("192.168.1.208", 8899);

        PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(socket.getOutputStream()))));
        for (int i = 0; i < 10; i++) {
            out.println("[" + i + "] : this is a test String ");
            out.flush();
        }
        socket.close();
    }
}
