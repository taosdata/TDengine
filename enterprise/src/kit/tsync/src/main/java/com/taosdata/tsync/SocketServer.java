package com.taosdata.tsync;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {

    public static void main(String[] args) throws IOException {
        final int port = 8899;
        ServerSocket ss = new ServerSocket(port);
        Socket sock = ss.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(sock.getInputStream())));

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("RECV >>> " + line);
        }

        sock.close();
        ss.close();
    }

}
