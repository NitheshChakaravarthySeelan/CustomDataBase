package com.minidb.replication.transport;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Abstracts the network communication channel between master and slave.
 */
public interface SocketChannel extends AutoCloseable {

    void connect(String host, int port) throws IOException;

    void send(Message msg) throws IOException;

    void setOnMessage(Consumer<Message> handler);

    void startListening();

    @Override
    void close() throws IOException;

    boolean isConnected();
}
