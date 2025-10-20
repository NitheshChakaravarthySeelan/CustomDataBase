package com.minidb.replication;

import com.minidb.log.LogRecord;
import com.minidb.log.WALManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Reads WAL segments incrementally and sends them.
 */
public class LogStreamer implements Iterator<LogRecord>, AutoCloseable {

    private final WALManager walManager;
    private FileChannel channel;
    private LogRecord nextRecord;
    private final ByteBuffer buffer = ByteBuffer.allocate(8192); // Internal buffer

    public LogStreamer(WALManager walManager) {
        this.walManager = walManager;
        buffer.flip(); // Initially empty
    }

    public void startFrom(long lsn) throws IOException {
        this.channel = walManager.getChannelForRecovery();
        this.channel.position(0);
        
        // Inefficiently scan from the beginning to find the starting LSN
        while (true) {
            LogRecord record = readNextRecordInternal();
            if (record == null) {
                this.nextRecord = null;
                break;
            }
            if (record.getLsn() >= lsn) {
                this.nextRecord = record;
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    @Override
    public LogRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        LogRecord current = nextRecord;
        try {
            nextRecord = readNextRecordInternal();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return current;
    }

    private LogRecord readNextRecordInternal() throws IOException {
        while (true) {
            LogRecord record = LogRecord.deserialize(buffer);
            if (record != null) {
                return record;
            }

            // If buffer is exhausted or no full record can be deserialized, read more from channel
            buffer.compact(); // Prepare for writing
            int bytesRead = channel.read(buffer);
            buffer.flip(); // Prepare for reading

            if (bytesRead <= 0 && !buffer.hasRemaining()) {
                return null; // End of channel and buffer
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (channel != null) {
            channel.close();
        }
    }
}
