package com.minidb.replication.transport;

import com.minidb.replication.SyncProtocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Encapsulates a replication message, handling serialization and deserialization
 * according to the defined protocol.
 */
public class Message {

    // Frame: [4:frameLen] + [1:version] + [1:msgType] + [8:correlationId] + [N:payload] + [4:crc]
    private static final int HEADER_LENGTH = 1 + 1 + 8; // version + msgType + correlationId
    private static final int CRC_LENGTH = 4;

    private final byte version;
    private final SyncProtocol.MessageType msgType;
    private final long correlationId;
    private final byte[] payload;

    public Message(SyncProtocol.MessageType msgType, long correlationId, byte[] payload) {
        this(SyncProtocol.PROTOCOL_VERSION, msgType, correlationId, payload);
    }

    public Message(byte version, SyncProtocol.MessageType msgType, long correlationId, byte[] payload) {
        this.version = version;
        this.msgType = msgType;
        this.correlationId = correlationId;
        this.payload = payload;
    }

    public byte[] encode() {
        int payloadLen = payload != null ? payload.length : 0;
        int bodyLen = HEADER_LENGTH + payloadLen;
        int frameLen = bodyLen + CRC_LENGTH;

        ByteBuffer buffer = ByteBuffer.allocate(4 + frameLen);
        buffer.putInt(frameLen);
        buffer.put(version);
        buffer.put(msgType.getValue());
        buffer.putLong(correlationId);
        if (payload != null) {
            buffer.put(payload);
        }

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 4, bodyLen);
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    public static Message decode(ByteBuffer frameBuf) throws ProtocolException {
        if (frameBuf.remaining() < HEADER_LENGTH + CRC_LENGTH) {
            throw new ProtocolException("Frame buffer is too small for header and CRC.");
        }

        byte version = frameBuf.get();
        if (version != SyncProtocol.PROTOCOL_VERSION) {
            throw new ProtocolException("Incompatible protocol version: " + version);
        }

        SyncProtocol.MessageType msgType = SyncProtocol.MessageType.fromByte(frameBuf.get());
        long correlationId = frameBuf.getLong();

        int payloadLen = frameBuf.remaining() - CRC_LENGTH;
        if (payloadLen < 0) {
            throw new ProtocolException("Invalid payload length calculated.");
        }
        byte[] payload = new byte[payloadLen];
        frameBuf.get(payload);

        long receivedCrc = frameBuf.getInt() & 0xFFFFFFFFL;
        CRC32 crc = new CRC32();
        crc.update(frameBuf.array(), frameBuf.arrayOffset() + frameBuf.position() - payloadLen - HEADER_LENGTH - CRC_LENGTH, HEADER_LENGTH + payloadLen);

        if (crc.getValue() != receivedCrc) {
            throw new ProtocolException("CRC32 checksum mismatch.");
        }

        return new Message(version, msgType, correlationId, payload);
    }

    // Static factory methods

    public static Message hello(long slaveIdHash, long lastLSN, String metadataJson, long correlationId) {
        byte[] metadataBytes = metadataJson.getBytes(StandardCharsets.UTF_8);
        ByteBuffer payload = ByteBuffer.allocate(8 + 8 + 4 + metadataBytes.length);
        payload.putLong(slaveIdHash);
        payload.putLong(lastLSN);
        payload.putInt(metadataBytes.length);
        payload.put(metadataBytes);
        return new Message(SyncProtocol.MessageType.HELLO, correlationId, payload.array());
    }

    public static Message streamWal(long startLsn, List<byte[]> recs, long correlationId) {
        int totalRecsBytes = recs.stream().mapToInt(r -> 4 + r.length).sum();
        ByteBuffer payload = ByteBuffer.allocate(8 + 4 + totalRecsBytes);
        payload.putLong(startLsn);
        payload.putInt(recs.size());
        for (byte[] rec : recs) {
            payload.putInt(rec.length);
            payload.put(rec);
        }
        return new Message(SyncProtocol.MessageType.STREAM_WAL, correlationId, payload.array());
    }

    public static Message ack(long ackedLsn, long correlationId) {
        ByteBuffer payload = ByteBuffer.allocate(8 + 4 + 4); // ackedLsn + pendingCount + infoLen
        payload.putLong(ackedLsn);
        payload.putInt(0); // pendingCount
        payload.putInt(0); // infoLen
        return new Message(SyncProtocol.MessageType.ACK, correlationId, payload.array());
    }

    public static Message ping(long timestamp, long correlationId) {
        ByteBuffer payload = ByteBuffer.allocate(8);
        payload.putLong(timestamp);
        return new Message(SyncProtocol.MessageType.PING, correlationId, payload.array());
    }

    public static Message pong(long timestamp, long correlationId) {
        ByteBuffer payload = ByteBuffer.allocate(8);
        payload.putLong(timestamp);
        return new Message(SyncProtocol.MessageType.PONG, correlationId, payload.array());
    }

    public static Message resync(long fromLSN, String reason, long correlationId) {
        byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);
        ByteBuffer payload = ByteBuffer.allocate(8 + 4 + reasonBytes.length);
        payload.putLong(fromLSN);
        payload.putInt(reasonBytes.length);
        payload.put(reasonBytes);
        return new Message(SyncProtocol.MessageType.RESYNC, correlationId, payload.array());
    }

    // Getters

    public byte getVersion() {
        return version;
    }

    public SyncProtocol.MessageType getMsgType() {
        return msgType;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public ByteBuffer payloadAsBuffer() {
        return ByteBuffer.wrap(payload);
    }

    public long getAckedLsn() {
        if (msgType != SyncProtocol.MessageType.ACK) {
            throw new IllegalStateException("Not an ACK message");
        }
        return payloadAsBuffer().getLong();
    }
}
