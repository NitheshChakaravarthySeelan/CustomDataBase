package com.minidb.raft;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;

import com.minidb.log.WALManager;

public class Candidate extends RaftNode {

    private AtomicInteger votesReceived;

    public Candidate(String nodeId, List<String> peers, RaftStorage storage, RaftProtocol protocol, WALManager walManager) {
        super(nodeId, peers, storage, protocol, walManager);
    }

    @Override
    public CompletableFuture<String> propose(String command) {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally(new NotLeaderException(null));
        return future;
    }

    @Override
    protected void onBecomeCandidate() {
        System.out.println(nodeId + " is a candidate.");
        currentTerm++;
        votedFor = nodeId;
        storage.saveCurrentTerm(currentTerm);
        storage.saveVotedFor(votedFor);
        votesReceived = new AtomicInteger(1);
        requestVotes();
    }

    private void requestVotes() {
        for (String peer : peers) {
            if (!peer.equals(nodeId)) {
                int lastLogIndex = log.size() - 1;
                int lastLogTerm = lastLogIndex >= 0 ? log.get(lastLogIndex).getTerm() : 0;
                RequestVoteRequest request = new RequestVoteRequest(currentTerm, nodeId, lastLogIndex, lastLogTerm);
                protocol.sendRequestVote(peer, request, (response) -> {
                    handleVoteResponse(response);
                });
            }
        }
    }

    private synchronized void handleVoteResponse(RequestVoteResponse response) {
        if (response.isVoteGranted()) {
            if (votesReceived.incrementAndGet() > peers.size() / 2) {
                becomeLeader();
            }
        } else if (response.getTerm() > currentTerm) {
            becomeFollower(response.getTerm());
        }
    }

    @Override
    protected void tick() {
        // Election timeout logic is handled by the transition to candidate
    }

    @Override
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.getTerm() >= currentTerm) {
            becomeFollower(req.getTerm());
        }
        return new AppendEntriesResponse(currentTerm, false);
    }

    @Override
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.getTerm() > currentTerm) {
            becomeFollower(req.getTerm());
            return new RequestVoteResponse(currentTerm, false);
        }
        return new RequestVoteResponse(currentTerm, false);
    }

    @Override
    protected void onBecomeLeader() {
        System.out.println(nodeId + " is becoming a leader.");
    }

    @Override
    protected void onBecomeFollower() {
        System.out.println(nodeId + " is becoming a follower.");
    }
}
