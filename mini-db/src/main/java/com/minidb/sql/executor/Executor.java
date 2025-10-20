package com.minidb.sql.executor;

import com.minidb.sql.parser.ast.*;
import com.minidb.txn.LockManager;
import com.minidb.txn.Transaction;
import com.minidb.txn.TxnManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Executor {
    private TxnManager txnManager;
    private LockManager lockManager;
    private com.minidb.storage.RecordStorage recordStorage;

    private static final long LOCK_WAIT_MS = TimeUnit.SECONDS.toMillis(10);

    public Executor(TxnManager txnManager, com.minidb.log.WALManager walManager, LockManager lockManager, com.minidb.storage.RecordStorage recordStorage) {
        this.txnManager = txnManager;
        this.lockManager = lockManager;
        this.recordStorage = recordStorage;
    }

    public Result execute(Command cmd) {
        try {
            if (cmd instanceof InsertCommand) {
                return executeInsert((InsertCommand) cmd);
            } else if (cmd instanceof SelectCommand) {
                return executeSelect((SelectCommand) cmd);
            } else if (cmd instanceof DeleteCommand) {
                return executeDelete((DeleteCommand) cmd);
            }
            return Result.error("Unsupported command type");
        } catch (Exception e) {
            return Result.error(e.getMessage());
        }
    }

    private Result executeSelect(SelectCommand cmd) {
        if (!"kv".equalsIgnoreCase(cmd.getTableName())) {
            return Result.error("Unknown table: " + cmd.getTableName());
        }

        Predicate pred = cmd.getPredicate();
        Transaction dummy = txnManager.begin();
        try {
            if (pred instanceof EqualsPredicate) {
                String key = ((EqualsPredicate) pred).getValue();
                ResourceId rid = ResourceId.key(cmd.getTableName(), key);
                lockManager.acquireShared(dummy, rid, LOCK_WAIT_MS);
                com.minidb.storage.RecordsSerializer.Row row = recordStorage.fetchRecord(Integer.parseInt(key));
                if (row != null) {
                    return Result.ok(List.of(new Pair<>(row.values[0].toString(), row.values[1].toString())));
                } else {
                    return Result.ok(List.of());
                }

            } else if (pred instanceof BetweenPredicate) {
                return Result.error("Range scans not supported yet");
            } else {
                return Result.error("Unsupported predicate type");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return Result.error("Interrupted while acquiring lock");
        } catch (Exception e) {
            return Result.error("Select failed: " + e.getMessage());
        } finally {
            lockManager.releaseAll(dummy);
        }
    }

    private Result executeInsert(InsertCommand cmd) {
        if (!"kv".equalsIgnoreCase(cmd.getTableName())) {
            return Result.error("Unknown table: " + cmd.getTableName());
        }

        Transaction tx = txnManager.begin();
        ResourceId rid = ResourceId.key(cmd.getTableName(), cmd.getKeyLiteral());

        try {
            lockManager.acquireExclusive(tx, rid, LOCK_WAIT_MS);
            com.minidb.storage.RecordsSerializer.Row row = new com.minidb.storage.RecordsSerializer.Row(2);
            row.values[0] = Integer.parseInt(cmd.getKeyLiteral());
            row.values[1] = cmd.getValueLiteral();
            recordStorage.insertRecord(row, tx);
            txnManager.commit(tx);
            return Result.ok();
        } catch (Exception e) {
            safeAbort(tx);
            return Result.error("Insert failed: " + e.getMessage());
        } finally {
            lockManager.releaseAll(tx);
        }
    }

    private Result executeDelete(DeleteCommand cmd) {
        if (!"kv".equalsIgnoreCase(cmd.getTableName())) {
            return Result.error("Unknown table: " + cmd.getTableName());
        }
        if (!(cmd.getPredicate() instanceof EqualsPredicate)) {
            return Result.error("Delete only supports equals predicate");
        }
        String key = ((EqualsPredicate) cmd.getPredicate()).getValue();

        Transaction tx = txnManager.begin();
        ResourceId rid = ResourceId.key(cmd.getTableName(), key);

        try {
            lockManager.acquireExclusive(tx, rid, LOCK_WAIT_MS);
            recordStorage.deleteRecord(Integer.parseInt(key), tx);
            txnManager.commit(tx);
            return Result.ok();

        } catch (Exception e) {
            safeAbort(tx);
            return Result.error("Delete failed: " + e.getMessage());
        } finally {
            lockManager.releaseAll(tx);
        }
    }

    private void safeAbort(Transaction tx) {
        try {
            txnManager.abort(tx);
        } catch (Exception e) {
            System.err.println("Failed to abort tx " + (tx == null ? "null" : tx.getTxnId()) + ": " + e.getMessage());
        }
    }
}
