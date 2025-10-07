package com.minidb.txn;

import com.minidb.sql.executor.ResourceId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

    private final ConcurrentHashMap<ResourceId, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Transaction, Set<ResourceId>> transactionLocks = new ConcurrentHashMap<>();

    public void acquireShared(Transaction txn, ResourceId resourceId, long timeout) throws InterruptedException {
        locks.putIfAbsent(resourceId, new ReentrantReadWriteLock());
        ReentrantReadWriteLock lock = locks.get(resourceId);
        if (lock.readLock().tryLock(timeout, java.util.concurrent.TimeUnit.MILLISECONDS)) {
            transactionLocks.computeIfAbsent(txn, k -> new HashSet<>()).add(resourceId);
        } else {
            throw new InterruptedException("Timeout acquiring shared lock on " + resourceId);
        }
    }

    public void acquireExclusive(Transaction txn, ResourceId resourceId, long timeout) throws InterruptedException {
        locks.putIfAbsent(resourceId, new ReentrantReadWriteLock());
        ReentrantReadWriteLock lock = locks.get(resourceId);
        if (lock.writeLock().tryLock(timeout, java.util.concurrent.TimeUnit.MILLISECONDS)) {
            transactionLocks.computeIfAbsent(txn, k -> new HashSet<>()).add(resourceId);
        } else {
            throw new InterruptedException("Timeout acquiring exclusive lock on " + resourceId);
        }
    }

    public void releaseAll(Transaction txn) {
        Set<ResourceId> heldLocks = transactionLocks.remove(txn);
        if (heldLocks != null) {
            for (ResourceId resourceId : heldLocks) {
                ReentrantReadWriteLock lock = locks.get(resourceId);
                if (lock != null) {
                    if (lock.isWriteLockedByCurrentThread()) {
                        lock.writeLock().unlock();
                    } else {
                        lock.readLock().unlock();
                    }
                }
            }
        }
    }
}
