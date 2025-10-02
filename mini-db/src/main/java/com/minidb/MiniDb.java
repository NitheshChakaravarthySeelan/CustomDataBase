package com.minidb;

import com.minidb.storage.BufferPool;
import com.minidb.storage.PageManager;
import com.minidb.storage.RecordId;
import com.minidb.storage.RecordStorage;
import com.minidb.storage.RecordsSerializer;
import com.minidb.storage.RecordsSerializer.Column;
import com.minidb.storage.RecordsSerializer.ColumnType;
import com.minidb.storage.RecordsSerializer.Row;

public class MiniDb {

    public static void main(String[] args) throws java.io.IOException {
        // 1. Define the table schema
        Column[] columns = new Column[2];
        columns[0] = new Column();
        columns[0].name = "id";
        columns[0].type = ColumnType.INT;
        columns[1] = new Column();
        columns[1].name = "name";
        columns[1].type = ColumnType.STRING;

        // 2. Initialize the storage components
        PageManager pageManager = new PageManager("minidb.db", 4096);
        BufferPool bufferPool = new BufferPool(pageManager, 10);
        RecordsSerializer serializer = new RecordsSerializer(columns);
        RecordStorage recordStorage = new RecordStorage(bufferPool, serializer);

        // 3. Create a record and insert it
        Row row = new Row(2);
        row.values[0] = 123;
        row.values[1] = "Hello, MiniDB!";
        RecordId rid = recordStorage.insertRecord(row);

        // 4. Fetch the record and print it
        Row fetchedRow = recordStorage.fetchRecord(rid);
        System.out.println("Fetched record: id = " + fetchedRow.values[0] + ", name = " + fetchedRow.values[1]);

        // 5. Clean up
        bufferPool.flushAllPages();
        pageManager.close();
    }
}
