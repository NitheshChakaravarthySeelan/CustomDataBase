package com.minidb.storage;

import com.minidb.index.BPlusTree;
import com.minidb.log.LogRecord;
import com.minidb.log.WALManager;
import com.minidb.storage.RecordsSerializer.Row;

import java.io.IOException;

public class RecordStorage {

    private final BufferPool bufferPool;
    private final RecordsSerializer recordSerializer;
    private final WALManager walManager;
    private final BPlusTree<Integer, RecordId> index;

    public RecordStorage(BufferPool bufferPool, RecordsSerializer recordSerializer, WALManager walManager, BPlusTree<Integer, RecordId> index) {
        this.bufferPool = bufferPool;
        this.recordSerializer = recordSerializer;
        this.walManager = walManager;
        this.index = index;
    }

    public RecordId insertRecord(Row row) throws IOException {
        byte[] recordBytes = recordSerializer.serialize(row);
        Integer key = (Integer) row.values[0]; // Assuming PK is the first column

        // 1. Log the operation
        LogRecord logRecord = new LogRecord(0, LogRecord.OP_PUT, 0, null, recordBytes);
        long lsn = walManager.appendAndFlush(logRecord);

        // 2. Find a page with enough space and insert
        int pageId = 0; // Simplified page selection
        Page page = bufferPool.getPage(pageId);
        int slotId = -1;
        while ((slotId = page.insertRecord(recordBytes)) == -1) {
            bufferPool.unpinPage(pageId, false);
            pageId++;
            page = bufferPool.getPage(pageId);
        }
        bufferPool.unpinPage(pageId, true); // Mark page as dirty
        RecordId rid = new RecordId(pageId, slotId);

        // 3. Update index
        index.insert(key, rid);

        // 4. Log the DONE operation
        LogRecord doneRecord = new LogRecord(lsn, LogRecord.OP_DONE, 0, null, null);
        walManager.appendAndFlush(doneRecord);

        return rid;
    }

    public void deleteRecord(Integer key) throws IOException {
        RecordId rid = index.search(key);
        if (rid == null) return; // Key not found

        // 1. Log the operation
        byte[] keyBytes = rid.serialize();
        LogRecord logRecord = new LogRecord(0, LogRecord.OP_DELETE, 0, keyBytes, null);
        long lsn = walManager.appendAndFlush(logRecord);

        // 2. Delete from index and page
        index.delete(key);
        Page page = bufferPool.getPage(rid.getPageId());
        page.deleteRecord(rid.getSlotId());
        bufferPool.unpinPage(rid.getPageId(), true); // Mark page as dirty

        // 3. Log the DONE operation
        LogRecord doneRecord = new LogRecord(lsn, LogRecord.OP_DONE, 0, null, null);
        walManager.appendAndFlush(doneRecord);
    }

    public Row fetchRecord(Integer key) {
        RecordId rid = index.search(key);
        if (rid == null) return null;
        return fetchRecord(rid);
    }

    public Row fetchRecord(RecordId rid) {
        Page page = bufferPool.getPage(rid.getPageId());
        byte[] recordBytes = page.getRecord(rid.getSlotId());
        if (recordBytes == null) {
            bufferPool.unpinPage(rid.getPageId(), false);
            return null; // Record deleted or does not exist
        }
        Row row = recordSerializer.deserialize(recordBytes);
        bufferPool.unpinPage(rid.getPageId(), false);
        return row;
    }
    
    // This method is for recovery purposes and should not be logged.
    public void insertRecordForRecovery(byte[] recordBytes) {
        Row row = recordSerializer.deserialize(recordBytes);
        Integer key = (Integer) row.values[0];

        int pageId = 0; // Simplified page selection
        Page page = bufferPool.getPage(pageId);
        int slotId = -1;
        while ((slotId = page.insertRecord(recordBytes)) == -1) {
            bufferPool.unpinPage(pageId, false);
            pageId++;
            page = bufferPool.getPage(pageId);
        }
        bufferPool.unpinPage(pageId, true);
        RecordId rid = new RecordId(pageId, slotId);
        index.insert(key, rid);
    }

    // This method is for recovery purposes and should not be logged.
    public void deleteRecordForRecovery(RecordId rid) {
        // Fetch the page once and perform all operations.
        Page page = bufferPool.getPage(rid.getPageId());
        byte[] recordBytes = page.getRecord(rid.getSlotId());
        if (recordBytes == null) {
            bufferPool.unpinPage(rid.getPageId(), false);
            return; // Record already deleted or never existed.
        }
        Row row = recordSerializer.deserialize(recordBytes);
        Integer key = (Integer) row.values[0];
        index.delete(key);
        page.deleteRecord(rid.getSlotId());
        bufferPool.unpinPage(rid.getPageId(), true);
    }
}