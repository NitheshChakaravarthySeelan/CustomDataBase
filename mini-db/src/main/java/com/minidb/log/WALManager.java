package com.minidb.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class WALManager implements Closeable {

    private final File walFile;
    private final FileChannel channel;
    private final AtomicLong nextLsn = new AtomicLong(1);
    private final List<WALListener> listeners = new ArrayList<>();

    public WALManager(File walDir) throws IOException {
        this.walFile = new File(walDir, "wal-000000000000.log");
        RandomAccessFile file = new RandomAccessFile(walFile, "rw");
        this.channel = file.getChannel();
        // If file has content, we need to determine the next LSN
        if (channel.size() > 0) {
            recoverNextLsn();
        }
    }

    public synchronized void registerListener(WALListener listener) {
        listeners.add(listener);
    }

    private void recoverNextLsn() throws IOException {
        channel.position(0);
        long maxLsn = 0;

        while (true) {
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            if (channel.read(sizeBuffer) != 4) {
                break; // End of file or not enough bytes for size
            }
            sizeBuffer.flip();
            int recordSize = sizeBuffer.getInt();

            if (recordSize <= 0 || channel.position() + recordSize + 8 > channel.size()) {
                break; // Corrupt tail or incomplete record
            }

            ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize + 8);
            if (channel.read(recordBuffer) != recordSize + 8) {
                break; // Incomplete record
            }
            recordBuffer.flip();

            // Create a new buffer that includes the recordSize at the beginning
            ByteBuffer fullRecordBuffer = ByteBuffer.allocate(4 + recordSize + 8);
            fullRecordBuffer.putInt(recordSize);
            fullRecordBuffer.put(recordBuffer);
            fullRecordBuffer.flip();

            LogRecord record = LogRecord.deserialize(fullRecordBuffer);

            if (record != null && record.isValid()) {
                maxLsn = Math.max(maxLsn, record.getLsn());
            } else {
                // Truncate corrupt tail
                channel.truncate(channel.position() - (4 + recordSize + 8)); // Rewind to before the corrupt record
                nextLsn.set(maxLsn + 1);
                return;
            }
        }
        nextLsn.set(maxLsn + 1);
    }

    public synchronized LogRecord append(LogRecord r) throws IOException {
        long lsn = nextLsn.getAndIncrement();
        LogRecord recordWithLsn = new LogRecord(lsn, r.getType(), r.getTxId(), r.getKey(), r.getValue());
        byte[] serialized = recordWithLsn.serialize();
        channel.write(ByteBuffer.wrap(serialized));
        return recordWithLsn;
    }

    public synchronized void flush() throws IOException {
        if (channel.isOpen()) {
            channel.force(true);
        }
    }

    public synchronized long appendAndFlush(LogRecord r) throws IOException {
        LogRecord recordWithLsn = append(r);
        flush();
        for (WALListener listener : listeners) {
            listener.onNewRecord(recordWithLsn);
        }
        return recordWithLsn.getLsn();
    }

    public synchronized void appendPredefined(LogRecord record) throws IOException {
        byte[] serialized = record.serialize();
        channel.write(ByteBuffer.wrap(serialized));
        // Ensure nextLsn is always ahead
        nextLsn.set(Math.max(nextLsn.get(), record.getLsn() + 1));
    }

    public synchronized void appendPredefinedAndFlush(LogRecord record) throws IOException {
        appendPredefined(record);
        flush();
    }

    public FileChannel getChannelForRecovery() throws IOException {
        return new RandomAccessFile(walFile, "r").getChannel();
    }

    public long getNextLsn() {
        return nextLsn.get();
    }

    public File getWalDir() {
        return walFile.getParentFile();
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
    }
}
