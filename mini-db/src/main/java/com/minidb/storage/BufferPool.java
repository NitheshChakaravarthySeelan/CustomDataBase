package com.minidb.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BufferPool {

    private final PageManager pageManager;
    private final int poolSize;
    private final Map<Integer, Page> pageCache;

    public BufferPool(PageManager pageManager, int poolSize) {
        this.pageManager = pageManager;
        this.poolSize = poolSize;
        this.pageCache = new HashMap<>();
    }

    public Page getPage(int pageId) {
        if (pageCache.containsKey(pageId)) {
            return pageCache.get(pageId);
        }

        // Page not in cache, so read it from disk
        byte[] pageBytes = new byte[4096]; // Assuming 4096 as page size
        try {
            pageManager.readPage(pageId, pageBytes);
        } catch (IOException e) {
            // Handle exception
            return null;
        }

        Page page = new Page(pageBytes, 100); // Assuming 100 max slots
        pageCache.put(pageId, page);
        return page;
    }

    public void unpinPage(int pageId, boolean isDirty) {
        if (isDirty) {
            // If the page is dirty, write it back to disk
            Page page = pageCache.get(pageId);
            if (page != null) {
                try {
                    pageManager.writePage(pageId, page.toBytes());
                } catch (IOException e) {
                    // Handle exception
                }
            }
        }
    }

    public void flushAllPages() {
        for (Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            if (entry.getValue().isDirty()) {
                unpinPage(entry.getKey(), true);
            }
        }
    }
}
