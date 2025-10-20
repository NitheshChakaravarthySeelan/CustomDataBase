package com.minidb.replication;

import com.minidb.log.LogRecord;
import com.minidb.log.WALManager;
import com.minidb.replication.transport.Message;
import com.minidb.replication.transport.SocketChannel;
import com.minidb.replication.transport.TcpSocketChannel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Slave-side component; applies incoming WAL stream.
 */
public class SlaveNode implements AutoCloseable {

    private final String masterHost;
    private final int masterPort;
    private final WALManager walManager;
    private final SocketChannel channel;
    private long lastAppliedLSN = 0;
    private volatile LogRecord lastReceivedRecord = null;
    private final List<LogRecord> receivedRecords = new ArrayList<>();
    private final File stateFile;
    private CountDownLatch recordsReceivedLatch;
    private long ackDelayMillis = 0; // New field for ACK delay

    public SlaveNode(String masterHost, int masterPort, WALManager walManager) throws IOException {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.walManager = walManager;
        this.channel = new TcpSocketChannel(masterHost, masterPort);
        this.stateFile = new File(walManager.getWalDir(), "slave_state.dat");
        loadState();
    }

    public void setRecordsReceivedLatch(CountDownLatch latch) {
        this.recordsReceivedLatch = latch;
    }

    public void setAckDelayMillis(long ackDelayMillis) {
        this.ackDelayMillis = ackDelayMillis;
    }

    public void start() throws IOException {
        channel.connect(masterHost, masterPort);
        channel.setOnMessage(this::handleMasterMessage);
        channel.startListening();

        // Send HELLO message
        Message hello = Message.hello(this.hashCode(), lastAppliedLSN, "{}", 0L);
        channel.send(hello);
        System.out.println("Slave connected to master and sent HELLO. Last applied LSN: " + lastAppliedLSN);
    }

    private void handleMasterMessage(Message message) {
        switch (message.getMsgType()) {
            case STREAM_WAL:
                handleStreamWal(message);
                break;
            case PONG:
                // handle PONG
                break;
            default:
                System.err.println("Unexpected message from master: " + message.getMsgType());
        }
    }

    private void handleStreamWal(Message message) {
        ByteBuffer payload = message.payloadAsBuffer();
        long startLSN = payload.getLong();
        int recordCount = payload.getInt();
        System.out.println("Slave received STREAM_WAL with " + recordCount + " records, starting at LSN=" + startLSN);
        long maxLsnInBatch = -1L;

        for (int i = 0; i < recordCount; i++) {
            int recLen = payload.getInt();
            byte[] recBytes = new byte[recLen];
            payload.get(recBytes);

            LogRecord record = LogRecord.deserialize(ByteBuffer.wrap(recBytes));
            if (record != null) {
                try {
                    walManager.appendPredefinedAndFlush(record);
                    this.lastReceivedRecord = record;
                    this.receivedRecords.add(record);
                    this.lastAppliedLSN = record.getLsn();
                    maxLsnInBatch = Math.max(maxLsnInBatch, record.getLsn());
                    if (recordsReceivedLatch != null) {
                        recordsReceivedLatch.countDown();
                    }
                } catch (Exception e) {
                    System.err.println("Error applying WAL record: " + e.getMessage());
                    // handle error, maybe request resync
                    return;
                }
            }
        }

        if (maxLsnInBatch > 0) {
            try {
                if (ackDelayMillis > 0) {
                    Thread.sleep(ackDelayMillis);
                }
                channel.send(Message.ack(maxLsnInBatch, 0L));
                saveState(); // Save state after ACK
            } catch (IOException | InterruptedException e) {
                System.err.println("Failed to send ACK to master or interrupted: " + e.getMessage());
            }
        }
    }

    private void saveState() throws IOException {
        Properties props = new Properties();
        props.setProperty("lastAppliedLSN", String.valueOf(lastAppliedLSN));
        try (FileOutputStream fos = new FileOutputStream(stateFile)) {
            props.store(fos, "SlaveNode state");
        }
    }

    private void loadState() throws IOException {
        if (stateFile.exists()) {
            Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream(stateFile)) {
                props.load(fis);
                this.lastAppliedLSN = Long.parseLong(props.getProperty("lastAppliedLSN", "0"));
            }
        }
    }

    public long getLastAppliedLSN() {
        return lastAppliedLSN;
    }

    public LogRecord getLastReceivedRecord() {
        return lastReceivedRecord;
    }

    public List<LogRecord> getReceivedRecords() {
        return receivedRecords;
    }

    @Override
    public void close() throws Exception {
        saveState(); // Save state on close
        channel.close();
    }
}
