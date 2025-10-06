package com.minidb.sql.executor;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.minidb.index.BPlusTree;
import com.minidb.index.Serializer;
import com.minidb.log.WALManager;
import com.minidb.sql.parser.ast.Command;
import com.minidb.sql.parser.ast.DeleteCommand;
import com.minidb.sql.parser.ast.InsertCommand;
import com.minidb.sql.parser.ast.Predicate;
import com.minidb.sql.parser.ast.SelectCommand;
import com.minidb.txn.LockTable;
import com.minidb.txn.Transaction;
import com.minidb.txn.TxnManager;

public class Executor {
    private TxnManager txnManager;
    private WALManager walManager;
    private BPlusTree<String, String> bPlusTree;
    private LockTable lockTable;
    private Serializer<String> serializer;

    private static final long LOCK_WAIT_MS = TimeUnit.SECONDS.toMillis(10);

    public Executor(TxnManager txnManager, WALManager walManager, BPlusTree<String, String> bPlusTree, LockTable lockTable, Serializer<String> serializer) {
        this.txnManager = txnManager;
        this.walManager = walManager;
        this.bPlusTree = bPlusTree;
        this.lockTable = lockTable;
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
        }
    }
    
    private Result executeSelect(SelectCommand cmd) {
        // validate table
        if (!"kv".equalsIgnoreCase(cmd.getTableString())) {
            return Result.error("Unknown table: " + cmd.getTableString());
        }

        Predicate pred = cmd.getPredicate();
        try {
            if (pred instanceof EqualsPredicate) {
                String key = ((EqualsPredicate) pred).getValue();
                // Acquire shared lock at key-level (read-committed semantics: release after read)
                ResourceId rid = ResourceId.key(cmd.getTableName(), key);
                Transaction dummy = txnManager.begin(); // optional, used purely for lock ownership
                try {
                    lockManager.acquireShared(dummy, rid, LOCK_WAIT_MS);
                    Optional<String> valOpt = bpt.search(key);
                    lockManager.releaseAll(dummy);
                    if (valOpt.isPresent()) {
                        var row = new Pair<String,String>(key, valOpt.get());
                        return Result.ok(List.of(row));
                    } else {
                        return Result.ok(List.of()); // empty result
                    }
                } finally {
                    lockManager.releaseAll(dummy);
                }

            } else if (pred instanceof BetweenPredicate) {
                BetweenPredicate bp = (BetweenPredicate) pred;
                String low = bp.getLow();
                String high = bp.getHigh();
                // For range scan, we take a table-level shared lock for simplicity
                Transaction dummy = txnManager.begin();
                ResourceId tableRid = ResourceId.table(cmd.getTableName());
                try {
                    lockManager.acquireShared(dummy, tableRid, LOCK_WAIT_MS);
                    List<Pair<String,String>> rows = bpt.rangeSearch(low, high);
                    lockManager.releaseAll(dummy);
                    return Result.ok(rows);
                } finally {
                    lockManager.releaseAll(dummy);
                }
            } else {
                return Result.error("Unsupported predicate type");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return Result.error("Interrupted while acquiring lock");
        } catch (Exception e) {
            return Result.error("Select failed: " + e.getMessage());
        }
    }

    /* ---------------------------
       INSERT implementation (auto-commit)
       --------------------------- */

    private Result executeInsert(InsertCommand cmd) {
        if (!"kv".equalsIgnoreCase(cmd.getTableName())) {
            return Result.error("Unknown table: " + cmd.getTableName());
        }

        Transaction tx = txnManager.begin();
        ResourceId rid = ResourceId.key(cmd.getTableName(), cmd.getKeyLiteral());

        try {
            // 1) acquire exclusive lock on key
            lockManager.acquireExclusive(tx, rid, LOCK_WAIT_MS);

            // 2) read before-image (if any)
            Optional<String> beforeOpt = bpt.search(cmd.getKeyLiteral());

            // 3) build and append WAL record (PUT)
            byte[] keyBytes = serializer.serialize(cmd.getKeyLiteral());
            byte[] valueBytes = serializer.serialize(cmd.getValueLiteral());
            LogRecord putRec = new LogRecord(0L, LogRecord.OP_PUT, tx.getId(), keyBytes, valueBytes);
            long lsn = walManager.append(putRec); // assign LSN
            // Optionally: store lsn into page later when applying (page-level code)

            // 4) apply the change to the index/storage
            bpt.insert(cmd.getKeyLiteral(), cmd.getValueLiteral());

            // 5) commit: write commit record and flush WAL
            LogRecord commitRec = new LogRecord(0L, LogRecord.OP_DONE, tx.getId(), null, null);
            walManager.appendAndFlush(commitRec);

            // 6) let txn manager mark commit and release locks
            txnManager.commit(tx);
            lockManager.releaseAll(tx);

            return Result.ok();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            safeAbort(tx);
            return Result.error("Interrupted while acquiring lock");
        } catch (Exception e) {
            safeAbort(tx);
            return Result.error("Insert failed: " + e.getMessage());
        } finally {
            // ensure locks released
            lockManager.releaseAll(tx);
        }
    }

    /* ---------------------------
       DELETE implementation (auto-commit)
       --------------------------- */

    private Result executeDelete(DeleteCommand cmd) {
        if (!"kv".equalsIgnoreCase(cmd.getTableName())) {
            return Result.error("Unknown table: " + cmd.getTableName());
        }

        Transaction tx = txnManager.begin();
        ResourceId rid = ResourceId.key(cmd.getTableName(), cmd.getKeyLiteral());

        try {
            // 1) acquire exclusive lock on key
            lockManager.acquireExclusive(tx, rid, LOCK_WAIT_MS);

            // 2) read before-image (must exist)
            Optional<String> beforeOpt = bpt.search(cmd.getKeyLiteral());
            if (beforeOpt.isEmpty()) {
                safeAbort(tx);
                return Result.error("Key not found: " + cmd.getKeyLiteral());
            }

            // 3) append delete record to WAL
            byte[] keyBytes = serializer.serialize(cmd.getKeyLiteral());
            LogRecord delRec = new LogRecord(0L, LogRecord.OP_DELETE, tx.getId(), keyBytes, null);
            long lsn = walManager.append(delRec);

            // 4) apply deletion to storage
            boolean deleted = bpt.delete(cmd.getKeyLiteral());
            if (!deleted) {
                safeAbort(tx);
                return Result.error("Delete failed for key: " + cmd.getKeyLiteral());
            }

            // 5) commit (append + flush commit)
            LogRecord commitRec = new LogRecord(0L, LogRecord.OP_DONE, tx.getId(), null, null);
            walManager.appendAndFlush(commitRec);

            txnManager.commit(tx);
            lockManager.releaseAll(tx);
            return Result.ok();

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            safeAbort(tx);
            return Result.error("Interrupted while acquiring lock");
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
            // best-effort: log; we can't throw from here
            System.err.println("Failed to abort tx " + (tx == null ? "null" : tx.getId()) + ": " + e.getMessage());
        } finally {
            if (tx != null) lockManager.releaseAll(tx);
        }
    }
}


