package com.minidb.replication.transport;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class TcpSocketChannel implements SocketChannel {

    private final String host;
    private final int port;
    private Socket socket;
    private OutputStream out;
    private DataInputStream in;
    private Consumer<Message> onMessage;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
    private volatile boolean isRunning = true;

    public TcpSocketChannel(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public TcpSocketChannel(Socket socket) throws IOException {
        this.socket = socket;
        this.host = socket.getInetAddress().getHostAddress();
        this.port = socket.getPort();
        this.out = socket.getOutputStream();
        this.in = new DataInputStream(socket.getInputStream());
    }

    @Override
    public void connect(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.out = socket.getOutputStream();
        this.in = new DataInputStream(socket.getInputStream());
    }

    @Override
    public void send(Message msg) throws IOException {
        writeLock.lock();
        try {
            if (!isConnected()) throw new IOException("Socket is not connected.");
            byte[] encoded = msg.encode();
            out.write(encoded);
            out.flush();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void setOnMessage(Consumer<Message> handler) {
        this.onMessage = handler;
    }

    @Override
    public void startListening() {
        listenerExecutor.submit(this::readLoop);
    }

    private void readLoop() {
        try {
            while (isRunning && !Thread.currentThread().isInterrupted() && isConnected()) {
                int frameLen = in.readInt();

                if (frameLen <= 0 || frameLen > 16 * 1024 * 1024) { // 16MB limit
                    throw new ProtocolException("Invalid frame length: " + frameLen);
                }

                byte[] frameBytes = new byte[frameLen];
                in.readFully(frameBytes);

                ByteBuffer frameBuffer = ByteBuffer.wrap(frameBytes);
                Message message = Message.decode(frameBuffer);

                if (onMessage != null) {
                    onMessage.accept(message);
                }
            }
        } catch (IOException | ProtocolException e) {
            if (isRunning) {
                System.err.println("Error in read loop: " + e.getMessage());
                try {
                    close();
                } catch (IOException ex) {
                    // ignore
                }
            }
        }
    }

    @Override
    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    @Override
    public void close() throws IOException {
        if (!isRunning) return;
        isRunning = false;
        listenerExecutor.shutdownNow();
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e) {
                // Log and ignore
            }
        }
    }
}
