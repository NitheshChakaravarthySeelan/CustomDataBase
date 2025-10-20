package com.minidb.replication;

import com.minidb.log.LogRecord;
import com.minidb.log.WALListener;
import com.minidb.log.WALManager;
import com.minidb.replication.transport.Message;
import com.minidb.replication.transport.ProtocolException;
import com.minidb.replication.transport.SocketChannel;
import com.minidb.replication.transport.TcpSocketChannel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Master-side coordinator; streams WAL entries to slaves.
 */
public class Replicator implements WALListener, AutoCloseable {
    private final LogStreamer logStreamer;
    private final Map<String, ReplicationState> slaves = new ConcurrentHashMap<>();
    private final ExecutorService replicationExecutor = Executors.newCachedThreadPool();
    private final int port;
    private ServerSocket serverSocket;
    private volatile boolean isRunning = true;
    private volatile LogRecord lastReceivedRecord = null;
    private final WALManager walManager;

    public Replicator(WALManager walManager, int port) {
        this.walManager = walManager;
        this.port = port;
        this.logStreamer = new LogStreamer(walManager);
    }

    public void start() throws IOException {
        walManager.registerListener(this);
        serverSocket = new ServerSocket(port);
        replicationExecutor.submit(this::acceptConnections);
        System.out.println("Replicator started on port " + port);
    }

    private void acceptConnections() {
        while (isRunning && !Thread.currentThread().isInterrupted()) {
            try {
                Socket slaveSocket = serverSocket.accept();
                replicationExecutor.submit(() -> handleNewSlave(slaveSocket));
            } catch (IOException e) {
                if (isRunning) {
                    System.err.println("Error accepting slave connection: " + e.getMessage());
                }
            }
        }
    }

    private void handleNewSlave(Socket slaveSocket) {
        try {
            SocketChannel channel = new TcpSocketChannel(slaveSocket);
            channel.setOnMessage(message -> handleSlaveMessage(channel, message));
            channel.startListening();
        } catch (IOException e) {
            System.err.println("Failed to establish connection with slave: " + e.getMessage());
        }
    }

    private void handleSlaveMessage(SocketChannel channel, Message message) {
        try {
            switch (message.getMsgType()) {
                case HELLO:
                    handleHello(channel, message);
                    break;
                case ACK:
                    handleAck(channel, message);
                    break;
                default:
                    System.err.println("Unexpected message from slave: " + message.getMsgType());
            }
        } catch (Exception e) {
            System.err.println("Error processing message from slave: " + e.getMessage());
            try {
                channel.close();
            } catch (IOException ioException) {
                // ignore
            }
        }
    }

    private void handleAck(SocketChannel channel, Message message) {
        long ackedLsn = message.getAckedLsn();
        // Find slave by channel
        for (ReplicationState state : slaves.values()) {
            if (state.getChannel() == channel) {
                state.setLastAckLSN(ackedLsn);
                state.setLastHeartbeat(System.currentTimeMillis());
                System.out.println("Received ACK from slave " + state.getSlaveId() + " for LSN=" + ackedLsn);
                return;
            }
        }
    }

    private void handleHello(SocketChannel channel, Message message) {
        ByteBuffer payload = message.payloadAsBuffer();
        long slaveIdHash = payload.getLong();
        long lastLSN = payload.getLong();
        String slaveId = String.valueOf(slaveIdHash);

        System.out.println("Received HELLO from slave " + slaveId + " with lastLSN=" + lastLSN);

        ReplicationState state = new ReplicationState(slaveId, channel, lastLSN);
        slaves.put(slaveId, state);

        // Stream historical records if the slave is behind
        replicationExecutor.submit(() -> streamHistoricalRecords(state, lastLSN));
    }

    private void streamHistoricalRecords(ReplicationState slave, long fromLsn) {
        long currentMasterLsn = walManager.getNextLsn();
        if (fromLsn < currentMasterLsn - 1) {
            System.out.println("Slave " + slave.getSlaveId() + " is behind. Streaming records from LSN=" + (fromLsn + 1));
            try (LogStreamer streamer = new LogStreamer(walManager)) {
                streamer.startFrom(fromLsn + 1);
                while (streamer.hasNext()) {
                    LogRecord record = streamer.next();
                    sendRecordToSlave(slave, record);
                }
            } catch (Exception e) {
                System.err.println("Error streaming historical records to slave " + slave.getSlaveId() + ": " + e.getMessage());
                slave.setActive(false);
            }
        }
    }

    @Override
    public void onNewRecord(LogRecord record) {
        this.lastReceivedRecord = record;
        System.out.println("[Replicator] Got new record: " + record.getLsn());
        for (ReplicationState slave : slaves.values()) {
            if (slave.isActive()) {
                replicationExecutor.submit(() -> sendRecordToSlave(slave, record));
            }
        }
    }

    private void sendRecordToSlave(ReplicationState slave, LogRecord record) {
        try {
            Message walMessage = Message.streamWal(record.getLsn(), Collections.singletonList(record.serialize()), 0L);
            slave.getChannel().send(walMessage);
            slave.setLastSentLSN(record.getLsn());
        } catch (IOException e) {
            System.err.println("Failed to send record to slave " + slave.getSlaveId() + ": " + e.getMessage());
            slave.setActive(false);
            // You might want to close the channel here as well
        }
    }

    public LogRecord getLastReceivedRecord() {
        return lastReceivedRecord;
    }

    public Map<String, ReplicationState> getSlaves() {
        return slaves;
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        for (ReplicationState slave : slaves.values()) {
            slave.getChannel().close();
        }
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        replicationExecutor.shutdownNow();
    }
}
