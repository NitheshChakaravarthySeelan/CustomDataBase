package com.minidb.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 *  A page is pinned while clients use it; pinned pages cannot be evicted.
    A page is dirty if modified in memory and not yet flushed to disk.
    Eviction chooses an unpinned victim; if dirty → flush to disk before evict.
    Caller responsibility: obtain page via getPage (which pins), perform changes (log before applying per WAL), then unpin/release.
    BufferPool does not itself apply WAL; it must expose hooks and ensure flush order when asked to flush.

    * Need to replace the LRU with the CLOCK *
 */
public class BufferPool {
    private static class PageFrame {
        Page page;
        final AtomicInteger pinCount = new AtomicInteger(0);
        final AtomicBoolean dirty = new AtomicBoolean(false);
        long lastAccessTime;
    }

	private final ConcurrentHashMap<Integer, PageFrame> pageFrames;
	private final LinkedHashMap<Integer, PageFrame> lru;
	private final int maxPages;
	private final PageManager pageManager;
	private final ReentrantLock lruLock; // Lock specifically for the LRU list

	public BufferPool(int maxPages, PageManager pageManager) {
		this.maxPages = maxPages;
		this.pageManager = pageManager;
		this.pageFrames = new ConcurrentHashMap<>(maxPages);
		// true for access-order, which is what we need for LRU
		this.lru = new LinkedHashMap<>(maxPages, 0.75f, true);
		this.lruLock = new ReentrantLock();
	}

	public Page getPage(int pageId) throws IOException {
		// computeIfAbsent is atomic and ensures loadPageFromDisk is called only once
		// even if multiple threads request the same page simultaneously.
		PageFrame frame = pageFrames.computeIfAbsent(pageId, id -> {
			try {
				return loadPageFromDisk(id);
			} catch (IOException e) {
				// To be handled properly in a real application
				throw new RuntimeException(e);
			}
		});

		// Pin the page and update its access time for LRU
		frame.pinCount.incrementAndGet();
		frame.lastAccessTime = System.nanoTime();
		
		// Update LRU list under its own lock
		lruLock.lock();
		try {
			lru.get(pageId);
		} finally {
			lruLock.unlock();
		}
		return frame.page;
	}

	private PageFrame loadPageFromDisk(int pageId) throws IOException {
		if (pageFrames.size() >= maxPages) {
			evictIfNeeded();
		}

		byte[] pageData = new byte[4096]; // Assuming PAGE_SIZE
		pageManager.readPage(pageId, pageData);
		Page page = new Page(pageData, 100); // Assuming maxSlots

		PageFrame frame = new PageFrame();
		frame.page = page;

		// Add to LRU list under lock
		lruLock.lock();
		try {
			lru.put(pageId, frame);
		} finally {
			lruLock.unlock();
		}
		return frame;
	}

	public void unpinPage(int pageId) throws IOException {
		PageFrame frame = pageFrames.get(pageId);
		if (frame == null) {
			throw new IOException("Page not in buffer pool: " + pageId);
		}

		// The page's own dirty flag is the source of truth.
		// Set the frame's dirty flag if the page has been modified.
		if (frame.page.isDirty()) {
			frame.dirty.set(true);
		}

		// Atomically decrement the pin count.
		frame.pinCount.decrementAndGet();
	}

	public void flushAll() throws IOException {
		// Iterate over a snapshot of the values to avoid concurrency issues
		// while still allowing other threads to access the pool.
		for (Map.Entry<Integer, PageFrame> entry : pageFrames.entrySet()) {
			PageFrame frame = entry.getValue();
			// Use compare-and-set to only flush if it's dirty, and then mark clean.
			if (frame.dirty.compareAndSet(true, false)) {
				pageManager.writePage(entry.getKey(), frame.page.toBytes());
			}
		}
	}

	public void close() throws IOException {
		// A lock is needed here to ensure no other operations are in-flight
		// during the final flush and close.
		lruLock.lock();
		try {
			flushAll();
		} finally {
			lruLock.unlock();
		}
		pageManager.close();
	}

	public void flushPage(int pageId) throws IOException {
		PageFrame frame = pageFrames.get(pageId);
		// Use compare-and-set to atomically check and update the dirty flag.
		if (frame != null && frame.dirty.compareAndSet(true, false)) {
			pageManager.writePage(pageId, frame.page.toBytes());
		}
	}

	private void evictIfNeeded() throws IOException {
		if (pageFrames.size() < maxPages) {
			return; // No need to evict
		}

		Integer victimId = null;
		PageFrame victimFrame = null;

		lruLock.lock();
		try {
			// Iterate through the LRU map to find the first unpinned page
			for (Map.Entry<Integer, PageFrame> entry : lru.entrySet()) {
				if (entry.getValue().pinCount.get() == 0) {
					victimId = entry.getKey();
					victimFrame = entry.getValue();
					break;
				}
			}

			if (victimId == null) {
				throw new IOException("Buffer pool is full and all pages are pinned. Cannot evict.");
			}

			// Remove the victim from the LRU list while holding the lock
			lru.remove(victimId);
		} finally {
			lruLock.unlock();
		}

		// The actual flush and removal from the main map can happen outside the LRU lock
		if (victimFrame.dirty.get()) {
			flushPage(victimId);
		}
		pageFrames.remove(victimId);
	}
}