package com.minidb.raft;

import java.util.ArrayList;
import java.util.List;

public class RaftStorage {

    private int currentTerm;
    private String votedFor;
    private List<LogEntry> log;
    private final String nodeId;

    public RaftStorage(String nodeId) {
        this.nodeId = nodeId;
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<>();
    }

    public synchronized int loadCurrentTerm() {
        return currentTerm;
    }

    public synchronized void saveCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public synchronized String loadVotedFor() {
        return votedFor;
    }

    public synchronized void saveVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public synchronized List<LogEntry> loadLog() {
        return new ArrayList<>(log);
    }

    public synchronized void saveLog(List<LogEntry> log) {
        this.log = new ArrayList<>(log);
    }
}
