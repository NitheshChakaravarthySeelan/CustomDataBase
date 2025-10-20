package com.minidb.replication;

/**
 * Defines the replication message types and protocol constants.
 */
public class SyncProtocol {

    public static final byte PROTOCOL_VERSION = 1;

    public enum MessageType {
        HELLO(1),
        STREAM_WAL(2),
        ACK(3),
        PING(4),
        PONG(5),
        RESYNC(6);

        private final byte value;

        MessageType(int value) {
            this.value = (byte) value;
        }

        public byte getValue() {
            return value;
        }

        public static MessageType fromByte(byte value) {
            for (MessageType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown message type value: " + value);
        }
    }
}
