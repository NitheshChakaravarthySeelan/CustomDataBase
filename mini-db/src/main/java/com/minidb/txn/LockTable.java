package com.minidb.txn;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LockTable {
    // 0 = FREE, -1 = EXCLUSIVE, >0 = SHARED count
    private final ConcurrentHashMap<Long, AtomicInteger> locks = new ConcurrentHashMap<>();

    // Read access multiple can hold it
    public boolean acquireShared(long resourceId) {
        locks.putIfAbsent(resourceId, new AtomicInteger(0));
        AtomicInteger state = locks.get(resourceId);

        while (true) {
            int current = state.get();
            if (current == -1) {
                // Exclusive lock held, spin or backoff
                Thread.yield();
                continue;
            }
            if (state.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    // Write Access only one can hold it
    public boolean acquireExclusive(long resourceId) {
        locks.putIfAbsent(resourceId, new AtomicInteger(0));
        AtomicInteger state = locks.get(resourceId);

        while (true) {
            if (state.compareAndSet(0, -1)) {
                return true;
            }
            // Someone else holds shared/exclusive lock
            Thread.yield();
        }
    }

    public void releaseShared(long resourceId) {
        AtomicInteger state = locks.get(resourceId);
        state.decrementAndGet();
    }

    public void releaseExclusive(long resourceId) {
        AtomicInteger state = locks.get(resourceId);
        state.compareAndSet(-1, 0);
    }
}
