package com.minidb;

import com.minidb.index.BPlusTree;
import com.minidb.log.RecoveryManager;
import com.minidb.log.WALManager;
import com.minidb.replication.RaftReplicator;
import com.minidb.sql.executor.Executor;
import com.minidb.storage.*;
import com.minidb.txn.LockManager;
import com.minidb.txn.TxnManager;

public class DbComponents {
    public PageManager pageManager;
    public BufferPool bufferPool;
    public WALManager walManager;
    public BPlusTree<Integer, RecordId> index;
    public LockManager lockManager;
    public TxnManager txnManager;
    public RecordsSerializer recordsSerializer;
    public RecordStorage recordStorage;
    public RaftReplicator replicator;
    public RecoveryManager recoveryManager;
    public Executor executor;

    public DbComponents(PageManager pageManager, BufferPool bufferPool, WALManager walManager, BPlusTree<Integer, RecordId> index, LockManager lockManager, TxnManager txnManager, RecordsSerializer recordsSerializer, RecordStorage recordStorage, RaftReplicator replicator, RecoveryManager recoveryManager, Executor executor) {
        this.pageManager = pageManager;
        this.bufferPool = bufferPool;
        this.walManager = walManager;
        this.index = index;
        this.lockManager = lockManager;
        this.txnManager = txnManager;
        this.recordsSerializer = recordsSerializer;
        this.recordStorage = recordStorage;
        this.replicator = replicator;
        this.recoveryManager = recoveryManager;
        this.executor = executor;
    }
}
