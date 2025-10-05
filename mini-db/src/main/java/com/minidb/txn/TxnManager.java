package com.minidb.txn;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.minidb.log.LogRecord;
import com.minidb.log.WALManager;

/**
 * Begin transaction allocate txnId and log it
 * Commit Transaction write commit record to the WAL, flush log, update status
 * Abort transaction log abort, trigger undo
 * Maintain a table of active transactions.
 * Should not directly communicate with the B+Tree only thorough WalManager and other abstracts
 */
public class TxnManager {
    private AtomicLong txnId;
    private WALManager walManager;
    private ConcurrentHashMap<Long, Transaction> activeTransactions;

    public TxnManager(WALManager walManager) {
        this.txnId = new AtomicLong(1);
        this.walManager = walManager;
        this.activeTransactions = new ConcurrentHashMap<>();
    }

    public long beginTransaction() {
        long txnId = this.txnId.getAndIncrement();
        Transaction txn = new Transaction(txnId, -1); // LSN will be set when the first log record is written
        txn.begin();
        activeTransactions.put(txnId, txn);
        return txnId;
    }

    public void commitTransaction(long txnId) {
        Transaction txn = activeTransactions.get(txnId);
        if (txn != null) {
            txn.commit();
            activeTransactions.remove(txnId);
        }
    }

    public void abortTransaction(long txnId) {
        Transaction txn = activeTransactions.get(txnId);
        if (txn != null) {
            txn.abort();
            activeTransactions.remove(txnId);
        }
    }

}
