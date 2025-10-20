package com.minidb.raft;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.CompletableFuture;

import com.minidb.log.WALManager;

public class Leader extends RaftNode {

    private Map<String, Integer> nextIndex;
    private Map<String, Integer> matchIndex;
    private Map<Integer, CompletableFuture<String>> clientRequests;

    public Leader(String nodeId, List<String> peers, RaftStorage storage, RaftProtocol protocol, WALManager walManager) {
        super(nodeId, peers, storage, protocol, walManager);
        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.clientRequests = new HashMap<>();
    }

    @Override
    public CompletableFuture<String> propose(String command) {
        LogEntry newEntry = new LogEntry(currentTerm, command);
        log.add(newEntry);
        storage.saveLog(log);
        CompletableFuture<String> future = new CompletableFuture<>();
        clientRequests.put(log.size() - 1, future);
        return future;
    }

    @Override
    protected void onBecomeLeader() {
        System.out.println(nodeId + " is the leader.");
        int lastLogIndex = log.size();
        for (String peer : peers) {
            nextIndex.put(peer, lastLogIndex + 1);
            matchIndex.put(peer, 0);
        }
        sendHeartbeats();
    }

    @Override
    protected void tick() {
        sendHeartbeats();
    }

    private void sendHeartbeats() {
        for (String peer : peers) {
            if (!peer.equals(nodeId)) {
                sendAppendEntries(peer);
            }
        }
    }

    private void sendAppendEntries(String peer) {
        int prevLogIndex = nextIndex.get(peer) - 1;
        int prevLogTerm = prevLogIndex >= 0 && prevLogIndex < log.size() ? log.get(prevLogIndex).getTerm() : 0;
        List<LogEntry> entries = log.subList(prevLogIndex + 1, log.size());

        AppendEntriesRequest request = new AppendEntriesRequest(currentTerm, nodeId, prevLogIndex, prevLogTerm, entries, commitIndex);
        protocol.sendAppendEntries(peer, request, (response) -> {
            handleAppendEntriesResponse(peer, response);
        });
    }

    private synchronized void handleAppendEntriesResponse(String peer, AppendEntriesResponse response) {
        if (response.isSuccess()) {
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, log.size() - 1);
            updateCommitIndex();
        } else {
            if (response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm());
            } else {
                nextIndex.put(peer, nextIndex.get(peer) - 1);
                sendAppendEntries(peer);
            }
        }
    }

    private void updateCommitIndex() {
        int majority = peers.size() / 2;
        for (int N = log.size() - 1; N > commitIndex; N--) {
            if (log.get(N).getTerm() == currentTerm) {
                int count = 1;
                for (String peer : peers) {
                    if (matchIndex.getOrDefault(peer, 0) >= N) {
                        count++;
                    }
                }
                if (count > majority) {
                    commitIndex = N;
                    for (int i = lastApplied + 1; i <= commitIndex; i++) {
                        applyStateMachine(log.get(i));
                        if (clientRequests.containsKey(i)) {
                            clientRequests.get(i).complete("Command processed: " + log.get(i).getCommand());
                            clientRequests.remove(i);
                        }
                    }
                    lastApplied = commitIndex;
                    break;
                }
            }
        }
    }

    @Override
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.getTerm() > currentTerm) {
            becomeFollower(req.getTerm());
        }
        return new AppendEntriesResponse(currentTerm, false);
    }

    @Override
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.getTerm() > currentTerm) {
            becomeFollower(req.getTerm());
            return handleRequestVote(req);
        }
        return new RequestVoteResponse(currentTerm, false);
    }

    @Override
    protected void onBecomeFollower() {
        System.out.println(nodeId + " is becoming a follower.");
    }

    @Override
    protected void onBecomeCandidate() {
        // A leader should not become a candidate
        throw new IllegalStateException("Leader cannot become a candidate");
    }
}
