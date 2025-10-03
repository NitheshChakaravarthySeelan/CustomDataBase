package com.minidb;

import com.minidb.index.BPlusTree;
import com.minidb.log.RecoveryManager;
import com.minidb.log.WALManager;
import com.minidb.serializers.IntegerSerializer;
import com.minidb.serializers.RecordIdSerializer;
import com.minidb.storage.*;
import com.minidb.storage.RecordsSerializer.Column;
import com.minidb.storage.RecordsSerializer.ColumnType;
import com.minidb.storage.RecordsSerializer.Row;

import java.io.File;
import java.io.IOException;

public class MiniDb {

    public static void main(String[] args) throws IOException {
        File dbDir = new File("minidb_data");
        if (!dbDir.exists()) {
            dbDir.mkdir();
        }

        // 1. Define the table schema
        Column[] columns = new Column[2];
        columns[0] = new Column();
        columns[0].name = "id";
        columns[0].type = ColumnType.INT;
        columns[1] = new Column();
        columns[1].name = "name";
        columns[1].type = ColumnType.STRING;

        // 2. Initialize managers and index
        PageManager pageManager = new PageManager(new File(dbDir, "minidb.db").getPath(), 4096);
        BufferPool bufferPool = new BufferPool(pageManager, 10);
        RecordsSerializer serializer = new RecordsSerializer(columns);
        WALManager walManager = new WALManager(dbDir);
        BPlusTree<Integer, RecordId> index = new BPlusTree<>(5, new IntegerSerializer(), new RecordIdSerializer());
        RecordStorage recordStorage = new RecordStorage(bufferPool, serializer, walManager, index);

        // 3. Recover from WAL
        RecoveryManager recoveryManager = new RecoveryManager(walManager, recordStorage);
        recoveryManager.recover();

        // 4. Insert some records
        System.out.println("Inserting records...");
        for (int i = 1; i <= 10; i++) {
            Row r = new Row(2);
            r.values[0] = i;
            r.values[1] = "value_" + i;
            recordStorage.insertRecord(r);
            System.out.println("Inserted: " + i);
        }

        // 5. Fetch a record using the index
        System.out.println("\nFetching record with ID 7...");
        Row fetchedRow = recordStorage.fetchRecord(7);
        if (fetchedRow != null) {
            System.out.println("Fetched record: id = " + fetchedRow.values[0] + ", name = " + fetchedRow.values[1]);
        } else {
            System.out.println("Record with ID 7 not found.");
        }

        // 6. Delete a record
        System.out.println("\nDeleting record with ID 7...");
        recordStorage.deleteRecord(7);
        Row deletedRow = recordStorage.fetchRecord(7);
        if (deletedRow == null) {
            System.out.println("Successfully deleted record with ID 7.");
        } else {
            System.out.println("Deletion failed.");
        }

        // 7. Clean up
        System.out.println("\nFlushing pages and closing DB...");
        bufferPool.flushAllPages();
        walManager.close();
        pageManager.close();
        System.out.println("DB closed.");
    }
}
