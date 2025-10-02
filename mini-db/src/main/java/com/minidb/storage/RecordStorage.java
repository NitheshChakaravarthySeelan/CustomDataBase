package com.minidb.storage;

import com.minidb.storage.RecordsSerializer.Row;

public class RecordStorage {

    private final BufferPool bufferPool;
    private final RecordsSerializer recordSerializer;

    public RecordStorage(BufferPool bufferPool, RecordsSerializer recordSerializer) {
        this.bufferPool = bufferPool;
        this.recordSerializer = recordSerializer;
    }

    public RecordId insertRecord(Row row) {
        // 1. Serialize the row
        byte[] recordBytes;
        try {
            recordBytes = recordSerializer.serialize(row);
        } catch (java.io.IOException e) {
            // Handle exception
            return null;
        }

        // 2. Find a page with enough space
        // This is a simplified approach. A real implementation would need a more
        // sophisticated way to find a page with free space.
        int pageId = 0; // Start with page 0 for now
        Page page = bufferPool.getPage(pageId);
        while (page.insertRecord(recordBytes) == -1) {
            bufferPool.unpinPage(pageId, false);
            pageId++;
            page = bufferPool.getPage(pageId);
        }

        // 3. Insert the record into the page
        int slotId = page.insertRecord(recordBytes);

        // 4. Unpin the page
        bufferPool.unpinPage(pageId, true);

        return new RecordId(pageId, slotId);
    }

    public Row fetchRecord(RecordId rid) {
        // 1. Get the page from the buffer pool
        Page page = bufferPool.getPage(rid.getPageId());

        // 2. Get the record bytes from the page
        byte[] recordBytes = page.getRecord(rid.getSlotId());

        // 3. Deserialize the record
        Row row = recordSerializer.deserialize(recordBytes);

        // 4. Unpin the page
        bufferPool.unpinPage(rid.getPageId(), false);

        return row;
    }
}