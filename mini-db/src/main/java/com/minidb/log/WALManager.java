package com.minidb.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

public class WALManager implements Closeable {

    private final File walFile;
    private final FileChannel channel;
    private final AtomicLong nextLsn = new AtomicLong(1);

    public WALManager(File walDir) throws IOException {
        this.walFile = new File(walDir, "wal-000000000000.log");
        RandomAccessFile file = new RandomAccessFile(walFile, "rw");
        this.channel = file.getChannel();
        // If file has content, we need to determine the next LSN
        if (channel.size() > 0) {
            recoverNextLsn();
        }
    }

    private void recoverNextLsn() throws IOException {
        channel.position(0);
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        long maxLsn = 0;

        while (channel.read(sizeBuffer) == 4) {
            sizeBuffer.flip();
            int recordSize = sizeBuffer.getInt();
            sizeBuffer.clear();

            if (recordSize <= 0 || channel.position() + recordSize + 8 > channel.size()) {
                // Truncate corrupt tail
                channel.truncate(channel.position() - 4);
                break;
            }

            ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize + 8);
            channel.read(recordBuffer);
            recordBuffer.flip();
            LogRecord record = LogRecord.deserialize(recordBuffer);

            if (record != null && record.isValid()) {
                maxLsn = Math.max(maxLsn, record.getLsn());
            } else {
                // Truncate corrupt tail
                channel.truncate(channel.position() - recordSize - 8 - 4);
                break;
            }
        }
        nextLsn.set(maxLsn + 1);
    }

    public synchronized long append(LogRecord r) throws IOException {
        long lsn = nextLsn.getAndIncrement();
        LogRecord recordWithLsn = new LogRecord(lsn, r.getType(), 0, r.getKey(), r.getValue());
        byte[] serialized = recordWithLsn.serialize();
        channel.write(ByteBuffer.wrap(serialized));
        return lsn;
    }

    public synchronized void flush() throws IOException {
        if (channel.isOpen()) {
            channel.force(true);
        }
    }

    public synchronized long appendAndFlush(LogRecord r) throws IOException {
        long lsn = append(r);
        flush();
        return lsn;
    }

    public FileChannel getChannelForRecovery() throws IOException {
        return new RandomAccessFile(walFile, "r").getChannel();
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
    }
}
