package com.minidb.log;

import com.minidb.storage.RecordId;
import com.minidb.storage.RecordStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RecoveryManager {

    private final WALManager walManager;
    private final RecordStorage recordStorage;

    public RecoveryManager(WALManager walManager, RecordStorage recordStorage) {
        this.walManager = walManager;
        this.recordStorage = recordStorage;
    }

    public void recover() throws IOException {
        Map<Long, LogRecord> records = new HashMap<>();
        Set<Long> doneLsns = new HashSet<>();

        // 1. Read all records from WAL
        try (FileChannel channel = walManager.getChannelForRecovery()) {
            channel.position(0);
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

            while (channel.read(sizeBuffer) == 4) {
                sizeBuffer.flip();
                int recordSize = sizeBuffer.getInt();
                sizeBuffer.clear();

                if (recordSize <= 0 || channel.position() + recordSize + 8 > channel.size()) {
                    break; // Corrupt tail
                }

                ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize + 8);
                channel.read(recordBuffer);
                recordBuffer.flip();
                LogRecord record = LogRecord.deserialize(recordBuffer);

                if (record != null && record.isValid()) {
                    records.put(record.getLsn(), record);
                    if (record.getType() == LogRecord.OP_DONE) {
                        doneLsns.add(record.getLsn());
                    }
                }
            }
        }

        // 2. Redo completed operations
        for (long lsn : doneLsns) {
            LogRecord recordToRedo = records.get(lsn);
            if (recordToRedo != null) {
                redo(recordToRedo);
            }
        }
    }

    private void redo(LogRecord record) {
        switch (record.getType()) {
            case LogRecord.OP_PUT:
                recordStorage.insertRecordForRecovery(record.getValue());
                break;
            case LogRecord.OP_DELETE:
                RecordId rid = RecordId.deserialize(record.getKey());
                recordStorage.deleteRecordForRecovery(rid);
                break;
        }
    }
}
