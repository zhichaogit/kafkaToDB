package com.esgyn.kafkaCDC.server.clientServer;

import lombok.Getter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class KCConnection {
    private static Logger LOG = Logger.getLogger(KCConnection.class);
    
    public static int KC_TRYCONNECT_TIMES = 3;

    private String        host;
    private int           port;
    @Getter
    private SocketChannel ch;
    private boolean bConnected = false;

    public KCConnection(String host, String port) throws Exception {
        this.host = host;
        this.port = Integer.parseInt(port);
        ch = SocketChannel.open();
        ch.configureBlocking(true);
        ch.socket().setSoTimeout(5000);
        checkConnection();
    }

    public KCConnection(SocketChannel ch) throws Exception {
        this.ch = ch;
        bConnected = ch.isConnected();
        if (bConnected) {
            this.host = ch.socket().getInetAddress().getHostName();
            this.port = ch.socket().getPort();
        } else {
            checkConnection();
        }
    }

    private void checkConnection() throws Exception {
        if (!bConnected) {
            bConnected = ch.connect(new InetSocketAddress(host, port));
            int waitCount = KC_TRYCONNECT_TIMES;
            while (!bConnected) {
                if (waitCount >= 0 && waitCount-- == 0) {
                    throw new Exception("Failed to connect IP:" + host + ", PORT:" + port);
                }
                bConnected = ch.finishConnect();
                Thread.sleep(10);
            }
        }
    }

    public void send(Message message) {
        try {
	    byte buf[] = message.getBytes();
            ByteBuffer bb = ByteBuffer.wrap(buf);
            do {
                ch.write(bb);
            } while (bb.hasRemaining());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            bConnected = false;
        }
    }

    public Message receive() throws Exception {
        try {
            int bytesRead = -1;
            ByteBuffer buffer = ByteBuffer.allocate(4);
            while (buffer.position() != 4) {
                bytesRead = ch.read(buffer);
                if (bytesRead == -1) {
                    bConnected = false;
                    throw new IOException("Connection is broken");
                }
            }
            byte[] data = buffer.array();
            int length = (int)((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) |
                    ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
            buffer = ByteBuffer.allocate(length);
            while (buffer.position() != buffer.limit()) {
                bytesRead = ch.read(buffer);
                if (bytesRead == -1) {
                    bConnected = false;
                    throw new IOException("Connection is broken");
                }
                ch.read(buffer);
            }
            return Message.decodeBytes(buffer.array());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
    }
}
