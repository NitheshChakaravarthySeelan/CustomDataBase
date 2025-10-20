package com.minidb.replication;

import com.minidb.log.LogRecord;
import com.minidb.log.WALListener;
import com.minidb.log.WALManager;
import com.minidb.raft.*;

import java.util.List;

public class RaftReplicator implements WALListener, AutoCloseable {

    private final WALManager walManager;
    private final RaftNode raftNode;
    private final RaftApi raftApi;

    public RaftReplicator(WALManager walManager, List<String> peerIds, String nodeId, RaftProtocol protocol) {
        this.walManager = walManager;
        this.raftNode = new Follower(nodeId, peerIds, new RaftStorage(nodeId), protocol, walManager);
        this.raftApi = new RaftApi(raftNode, protocol);
        walManager.registerListener(this);
    }

    public void start() {
        raftNode.start();
    }

    @Override
    public void onNewRecord(LogRecord record) {
        // This is called when a new record is written to the WAL.
        // We need to propose this record to the Raft cluster.
        // The leader will then replicate it to the followers.
        byte[] serializedRecord = record.serialize();
        raftApi.propose(new String(serializedRecord)); 
    }

    @Override
    public void close() throws Exception {
        raftNode.stop();
    }
}
