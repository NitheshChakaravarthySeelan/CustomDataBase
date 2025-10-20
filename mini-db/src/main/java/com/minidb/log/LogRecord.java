package com.minidb.log;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public final class LogRecord {
    public static final byte OP_PUT = 1;
    public static final byte OP_DELETE = 2;
    public static final byte OP_DONE = 3;
    public static final byte OP_CHECKPOINT = 4;

    private final int recordLen;
    private final long lsn;
    private final byte type;
    private final long txId;
    private final byte[] key;
    private final byte[] value;
    private final long checksum;

    public LogRecord(long lsn, byte type, long txId, byte[] key, byte[] value) {
        this.lsn = lsn;
        this.type = type;
        this.txId = txId;
        this.key = key;
        this.value = value;

        int len = 8 /* lsn */ + 1 /* type */ + 8 /* txId */;
        if (key != null) {
            len += 4 /* keyLen */ + key.length;
        } else {
            len += 4;
        }
        if (value != null) {
            len += 4 /* valueLen */ + value.length;
        } else {
            len += 4 /* valueLen */;
        }
        this.recordLen = len;
        this.checksum = calculateChecksum();
    }

    private LogRecord(int recordLen, long lsn, byte type, long txId, byte[] key, byte[] value, long checksum) {
        this.recordLen = recordLen;
        this.lsn = lsn;
        this.type = type;
        this.txId = txId;
        this.key = key;
        this.value = value;
        this.checksum = checksum;
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + recordLen + 8);
        buffer.putInt(recordLen);
        buffer.putLong(lsn);
        buffer.put(type);
        buffer.putLong(txId);

        if (key != null) {
            buffer.putInt(key.length);
            buffer.put(key);
        } else {
            buffer.putInt(0);
        }

        if (value != null) {
            buffer.putInt(value.length);
            buffer.put(value);
        } else {
            buffer.putInt(0);
        }
        buffer.putLong(checksum);
        return buffer.array();
    }

    public static LogRecord deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            return null;
        }
        buffer.mark();
        int recordLen = buffer.getInt();
        if (buffer.remaining() < recordLen + 8) {
            buffer.reset();
            return null; // Not enough data for a full record
        }

        // Read payload
        byte[] payload = new byte[recordLen];
        buffer.get(payload);
        
        // Read checksum
        long checksum = buffer.getLong();

        // Now parse the payload
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        long lsn = payloadBuffer.getLong();
        byte type = payloadBuffer.get();
        long txId = payloadBuffer.getLong();

        int keyLen = payloadBuffer.getInt();
        byte[] key = new byte[keyLen];
        if (keyLen > 0) {
            payloadBuffer.get(key);
        }

        int valueLen = payloadBuffer.getInt();
        byte[] value = new byte[valueLen];
        if (valueLen > 0) {
            payloadBuffer.get(value);
        }

        return new LogRecord(recordLen, lsn, type, txId, key, value, checksum);
    }

    private long calculateChecksum() {
        CRC32 crc = new CRC32();
        ByteBuffer buffer = ByteBuffer.allocate(recordLen);
        buffer.putLong(lsn);
        buffer.put(type);
        buffer.putLong(txId);
        if (key != null) {
            buffer.putInt(key.length);
            buffer.put(key);
        } else {
            buffer.putInt(0);
        }
        if (value != null) {
            buffer.putInt(value.length);
            buffer.put(value);
        } else {
            buffer.putInt(0);
        }
        crc.update(buffer.array(), 0, recordLen);
        return crc.getValue();
    }

    public boolean isValid() {
        return calculateChecksum() == this.checksum;
    }

    // Getters
    public int getRecordLen() {
        return recordLen;
    }

    public long getLsn() {
        return lsn;
    }

    public byte getType() {
        return type;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getTxId() {
        return txId;
    }
}