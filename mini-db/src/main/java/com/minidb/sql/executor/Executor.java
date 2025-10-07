package com.minidb.sql.executor;

import com.minidb.index.BPlusTree;
import com.minidb.index.Serializer;
import com.minidb.log.LogRecord;
import com.minidb.log.WALManager;
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
    private WALManager walManager;
    private BPlusTree<String, String> bpt;
    private LockManager lockManager;
    private Serializer<String> serializer;

    private static final long LOCK_WAIT_MS = TimeUnit.SECONDS.toMillis(10);

    public Executor(TxnManager txnManager, WALManager walManager, BPlusTree<String, String> bPlusTree, LockManager lockManager, Serializer<String> serializer) {
        this.txnManager = txnManager;
        this.walManager = walManager;
        this.bpt = bPlusTree;
        this.lockManager = lockManager;
        this.serializer = serializer;
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
                Optional<String> valOpt = Optional.ofNullable(bpt.search(key));
                if (valOpt.isPresent()) {
                    var row = new Pair<String, String>(key, valOpt.get());
                    return Result.ok(List.of(row));
                } else {
                    return Result.ok(List.of());
                }

            } else if (pred instanceof BetweenPredicate) {
                BetweenPredicate bp = (BetweenPredicate) pred;
                String low = bp.getLow();
                String high = bp.getHigh();
                ResourceId tableRid = ResourceId.table(cmd.getTableName());
                lockManager.acquireShared(dummy, tableRid, LOCK_WAIT_MS);
                List<Pair<String, String>> rows = bpt.rangeSearch(low, high).stream().map(entry -> new Pair<>(entry.getKey(), entry.getValue())).collect(Collectors.toList());
                return Result.ok(rows);
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
            byte[] keyBytes = serializer.serialize(cmd.getKeyLiteral());
            byte[] valueBytes = serializer.serialize(cmd.getValueLiteral());
            LogRecord putRec = new LogRecord(0L, LogRecord.OP_PUT, tx.getTxnId(), keyBytes, valueBytes);
            walManager.append(putRec);
            bpt.insert(cmd.getKeyLiteral(), cmd.getValueLiteral());
            LogRecord commitRec = new LogRecord(0L, LogRecord.OP_DONE, tx.getTxnId(), null, null);
            walManager.appendAndFlush(commitRec);
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
            Optional<String> beforeOpt = Optional.ofNullable(bpt.search(key));
            if (beforeOpt.isEmpty()) {
                safeAbort(tx);
                return Result.error("Key not found: " + key);
            }

            byte[] keyBytes = serializer.serialize(key);
            LogRecord delRec = new LogRecord(0L, LogRecord.OP_DELETE, tx.getTxnId(), keyBytes, null);
            walManager.append(delRec);

            bpt.delete(key);

            LogRecord commitRec = new LogRecord(0L, LogRecord.OP_DONE, tx.getTxnId(), null, null);
            walManager.appendAndFlush(commitRec);

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
