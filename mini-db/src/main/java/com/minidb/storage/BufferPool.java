package com.minidb.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
public class BufferPool {

    private final PageManager pageManager;
    private final int poolSize;
    private final Map<Integer, Page> pageCache;

    public BufferPool(PageManager pageManager, int poolSize) {
        this.pageManager = pageManager;
        this.poolSize = poolSize;
        // Use LinkedHashMap for LRU cache behavior.
        // The 'true' in the constructor enables access-order, which is key for LRU.
        this.pageCache = new LinkedHashMap<Integer, Page>(poolSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Page> eldest) {
                // This condition is met when a new element is added and the map exceeds its capacity.
                if (size() > poolSize) {
                    // Before evicting, check if the page is dirty. If so, flush it.
                    // Also, a real buffer pool would check if pin_count is 0.
                    if (eldest.getValue().isDirty()) {
                        try {
                            pageManager.writePage(eldest.getKey(), eldest.getValue().toBytes());
                        } catch (IOException e) {
                            // In a real system, this is a critical error.
                            e.printStackTrace();
                        }
                    }
                    return true;
                }
                return false;
            }
        };
    }

    public Page getPage(int pageId) {
        if (pageCache.containsKey(pageId)) {
            Page page = pageCache.get(pageId);
            page.pin();
            return page;
        }

        // Page not in cache, so read it from disk
        // Eviction of an old page happens automatically here if the pool is full,
        // thanks to the LinkedHashMap's removeEldestEntry override.

        byte[] pageBytes = new byte[pageManager.getPageSize()];
        try {
            pageManager.readPage(pageId, pageBytes);
        } catch (IOException e) {
            e.printStackTrace(); // Or a more robust error handling
            return null;
        }

        // Calculate maxSlots dynamically instead of hardcoding
        int maxSlots = (pageManager.getPageSize() - Page.HEADER_SIZE) / Page.SLOT_ENTRY_SIZE;
        Page page = new Page(pageBytes, maxSlots);
        pageCache.put(pageId, page);
        // The page is already pinned with a count of 1 upon creation/loading.
        return page;
    }

    public void unpinPage(int pageId, boolean isDirty) {
        Page page = pageCache.get(pageId);
        if (page != null) {
            page.unpin();
            if (isDirty) {
                page.setDirty(true);
            }
            // A more advanced design might only flush when the page is evicted or during a checkpoint.
            // For simplicity, flushing immediately on unpinning a dirty page is also an option, but less efficient.
        }
    }

    public void flushAllPages() {
        for (Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            if (entry.getValue().isDirty()) {
                try {
                    pageManager.writePage(entry.getKey(), entry.getValue().toBytes());
                    entry.getValue().setDirty(false); // Mark as clean after flushing
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
