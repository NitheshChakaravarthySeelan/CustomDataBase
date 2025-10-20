package com.minidb.raft;

public class NotLeaderException extends RuntimeException {

    private final String leaderId;

    public NotLeaderException(String leaderId) {
        this.leaderId = leaderId;
    }

    public String getLeaderId() {
        return leaderId;
    }
}
