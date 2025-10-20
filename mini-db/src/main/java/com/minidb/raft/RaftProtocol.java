package com.minidb.raft;

import java.util.function.Consumer;

public interface RaftProtocol {
    void sendAppendEntries(String peer, AppendEntriesRequest request, Consumer<AppendEntriesResponse> callback);
    void sendRequestVote(String peer, RequestVoteRequest request, Consumer<RequestVoteResponse> callback);
    void setRaftNode(RaftNode node);
}
