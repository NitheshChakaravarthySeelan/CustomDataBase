package com.minidb.raft;

import java.util.concurrent.CompletableFuture;

public class RaftApi {

    private RaftNode raftNode;
    private final RaftProtocol protocol;

    public RaftApi(RaftNode raftNode, RaftProtocol protocol) {
        this.raftNode = raftNode;
        this.protocol = protocol;
    }

    public CompletableFuture<String> propose(String command) {
        return raftNode.propose(command)
                .handle((ok, ex) -> {
                    if (ex != null) {
                        if (ex.getCause() instanceof NotLeaderException) {
                            NotLeaderException notLeaderException = (NotLeaderException) ex.getCause();
                            String leaderId = notLeaderException.getLeaderId();
                            if (leaderId != null && protocol instanceof InMemoryRaftProtocol) {
                                RaftNode leaderNode = ((InMemoryRaftProtocol) protocol).getNode(leaderId);
                                if (leaderNode != null) {
                                    this.raftNode = leaderNode;
                                    return propose(command);
                                }
                            }
                        }
                        return CompletableFuture.<String>failedFuture(ex);
                    }
                    return CompletableFuture.completedFuture(ok);
                }).thenCompose(f -> f);
    }
}
