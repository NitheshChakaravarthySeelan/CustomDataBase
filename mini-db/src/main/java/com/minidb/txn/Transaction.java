package com.minidb.txn;

import java.util.List;

import com.minidb.log.WALManager;
import com.minidb.log.LogRecord;

public class Transaction {

    public enum Status {
        ACTIVE,
        COMMITTED,
        ABORTED
    }

    private final long txnId;
    private Status status;
    private long startLSN; // WAL Log Sequence Number
    private List<Long> pinnedResource;
    private long startTimestamp;
    private long commitTimestamp;
    private long abortTimestamp;
    private WALManager walManager;

    public Transaction(long txnId, long startLSN) {
        this.txnId = txnId;
        this.status = Status.ACTIVE;
        this.startLSN = startLSN;
        this.startTimestamp = System.currentTimeMillis();
        this.commitTimestamp = -1;
        this.abortTimestamp = -1;
    }

    public void begin() {
        this.status = Status.ACTIVE;
        this.startTimestamp = System.currentTimeMillis();
        this.commitTimestamp = -1;
        this.abortTimestamp = -1;
    }

    public long logUpdate(LogRecord logRecord) {
        try {
            return walManager.appendAndFlush(logRecord);
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public void addUndoEntry(LogRecord logRecord) {
        // Will be implemented later
    }

    public void registerLock(long resourceId) {
        pinnedResource.add(resourceId);
    }

    public void unregisterLock(long resourceId) {
        pinnedResource.remove(resourceId);
    }

    public void commit() {
        this.status = Status.COMMITTED;
        this.commitTimestamp = System.currentTimeMillis();
    }

    public void abort() {
        this.status = Status.ABORTED;
        this.abortTimestamp = System.currentTimeMillis();
    }
    
    public long getTxnId() {
        return txnId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getStartLSN() {
        return startLSN;
    }
}
