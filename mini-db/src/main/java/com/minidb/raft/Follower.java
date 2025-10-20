package com.minidb.raft;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import com.minidb.log.WALManager;

/**
 * Track the latest heartbeat from the leader
 * Transition to candidate if no heartbeat arrives within the election timeout
 * Respond to leader's log replication request (AppendEntries)
 * Respond to other candidate's vote request (RequestVote)
 * Maintain persistent state (currentTerm, votedFor, log)
 */
public class Follower extends RaftNode {
	private long lastHeartbeatTime;
    private final int minElectionTimeout = 150;
    private final int maxElectionTimeout = 300;
    private final Random random = new Random();
    private String leaderId;

	public Follower(String nodeId, List<String> peers, RaftStorage storage, RaftProtocol protocol, WALManager walManager) {
		super(nodeId, peers, storage, protocol, walManager);
		this.lastHeartbeatTime = System.currentTimeMillis();
	}

	@Override
	public CompletableFuture<String> propose(String command) {
		CompletableFuture<String> future = new CompletableFuture<>();
		future.completeExceptionally(new NotLeaderException(leaderId));
		return future;
	}

	@Override
	protected void onBecomeFollower() {
		// Reset the heartbeat
		this.lastHeartbeatTime = System.currentTimeMillis();
		System.out.println("The new reseted heart beat time of the follower is: " + this.lastHeartbeatTime);
	}

	@Override
	protected void tick() {
		// Called periodically by Raft Node Scheduler
		long now = System.currentTimeMillis();
		if (now - this.lastHeartbeatTime > getRandomElectionTimeout()) {
			becomeCandidate();
		}
	}

    private int getRandomElectionTimeout() {
        return minElectionTimeout + random.nextInt(maxElectionTimeout - minElectionTimeout);
    }

	@Override
	public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
		// Update the term if needed
        boolean success = false;
		if (req.getTerm() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false);
        }

        if (req.getTerm() > currentTerm) {
            becomeFollower(req.getTerm());
        }

        leaderId = req.getLeaderId();
        lastHeartbeatTime = System.currentTimeMillis();

        if (req.getPrevLogIndex() > 0 && (req.getPrevLogIndex() >= log.size() || log.get(req.getPrevLogIndex()).getTerm() != req.getPrevLogTerm())) {
            return new AppendEntriesResponse(currentTerm, false);
        }

        int index = req.getPrevLogIndex();
        for (LogEntry entry : req.getEntries()) {
            index++;
            if (index < log.size()) {
                if (log.get(index).getTerm() != entry.getTerm()) {
                    log.subList(index, log.size()).clear();
                    log.add(entry);
                }
            } else {
                log.add(entry);
            }
        }
        storage.saveLog(log);

        if (req.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(req.getLeaderCommit(), log.size() - 1);
        }
        success = true;

        return new AppendEntriesResponse(currentTerm, success);
	}

    @Override
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.getTerm() < currentTerm) {
            return new RequestVoteResponse(currentTerm, false);
        }

        if (req.getTerm() > currentTerm) {
            becomeFollower(req.getTerm());
        }

        boolean alreadyVoted = votedFor != null && !votedFor.equals(req.getCandidateId());
        boolean logIsUpToDate = isLogUpToDate(req.getLastLogIndex(), req.getLastLogTerm());

        if (!alreadyVoted && logIsUpToDate) {
            votedFor = req.getCandidateId();
            storage.saveVotedFor(votedFor);
            return new RequestVoteResponse(currentTerm, true);
        } else {
            return new RequestVoteResponse(currentTerm, false);
        }
    }

    private boolean isLogUpToDate(int lastLogIndex, int lastLogTerm) {
        if (log.isEmpty()) {
            return true;
        }
        int ourLastLogIndex = log.size() - 1;
        int ourLastLogTerm = log.get(ourLastLogIndex).getTerm();
        return lastLogTerm > ourLastLogTerm || (lastLogTerm == ourLastLogTerm && lastLogIndex >= ourLastLogIndex);
    }

    @Override
    protected void onBecomeLeader() {
        // A follower should not become a leader directly
        throw new IllegalStateException("Follower cannot become a leader directly");
    }

    @Override
    protected void onBecomeCandidate() {
        System.out.println(nodeId + " is becoming a candidate.");
    }
}
