package com.minidb.raft;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class InMemoryRaftProtocol implements RaftProtocol {

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();

    public void registerNode(RaftNode node) {
        nodes.put(node.nodeId, node);
        node.setRaftProtocol(this);
    }

    public RaftNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    @Override
    public void sendAppendEntries(String peer, AppendEntriesRequest request, Consumer<AppendEntriesResponse> callback) {
        RaftNode peerNode = nodes.get(peer);
        if (peerNode != null) {
            AppendEntriesResponse response = peerNode.handleAppendEntries(request);
            callback.accept(response);
        }
    }

    @Override
    public void sendRequestVote(String peer, RequestVoteRequest request, Consumer<RequestVoteResponse> callback) {
        RaftNode peerNode = nodes.get(peer);
        if (peerNode != null) {
            RequestVoteResponse response = peerNode.handleRequestVote(request);
            callback.accept(response);
        }
    }

    @Override
    public void setRaftNode(RaftNode node) {
        // Not needed for this implementation
    }
}
