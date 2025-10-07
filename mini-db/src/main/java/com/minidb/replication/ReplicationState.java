package com.minidb.replication;

import com.minidb.replication.transport.SocketChannel;

public class ReplicationState {

    private final String slaveId;
    private final SocketChannel channel;
    private long lastAckLSN;
    private long lastSentLSN;
    private long lastHeartbeat;
    private boolean isActive;

    public ReplicationState(String slaveId, SocketChannel channel, long initialLsn) {
        this.slaveId = slaveId;
        this.channel = channel;
        this.lastAckLSN = initialLsn;
        this.lastSentLSN = initialLsn;
        this.lastHeartbeat = System.currentTimeMillis();
        this.isActive = true;
    }

    public String getSlaveId() {
        return slaveId;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public long getLastAckLSN() {
        return lastAckLSN;
    }

    public void setLastAckLSN(long lastAckLSN) {
        this.lastAckLSN = lastAckLSN;
    }

    public long getLastSentLSN() {
        return lastSentLSN;
    }

    public void setLastSentLSN(long lastSentLSN) {
        this.lastSentLSN = lastSentLSN;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }
}
