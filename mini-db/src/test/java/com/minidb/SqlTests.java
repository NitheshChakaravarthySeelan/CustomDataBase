package com.minidb;

import com.minidb.index.BPlusTree;
import com.minidb.index.Serializer;
import com.minidb.log.WALManager;
import com.minidb.serializers.IntegerSerializer;
import com.minidb.serializers.RecordIdSerializer;
import com.minidb.sql.executor.Executor;
import com.minidb.sql.executor.Result;
import com.minidb.sql.parser.Parser;
import com.minidb.sql.parser.Token;
import com.minidb.sql.parser.Tokenizer;
import com.minidb.storage.*;
import com.minidb.txn.LockManager;
import com.minidb.txn.TxnManager;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class SqlTests {

    private Executor executor;

    @Before
    public void setup() throws IOException {
        File dbDir = new File("minidb_test_data");
        if (!dbDir.exists()) {
            dbDir.mkdir();
        }
        PageManager pageManager = new PageManager(new File(dbDir, "minidb.db").getPath(), 4096);
        BufferPool bufferPool = new BufferPool(pageManager, 10);
        WALManager walManager = new WALManager(dbDir);
        Serializer<Integer> keySerializer = new IntegerSerializer();
        Serializer<RecordId> valueSerializer = new RecordIdSerializer();
        BPlusTree<Integer, RecordId> index = new BPlusTree<>(5, keySerializer, valueSerializer, pageManager, bufferPool);
        System.out.println("SqlTests.setup: BPlusTree index hashcode: " + index.hashCode());
        LockManager lockManager = new LockManager();
        TxnManager txnManager = new TxnManager(lockManager, walManager);
        RecordsSerializer recordsSerializer = new RecordsSerializer(new RecordsSerializer.Column[]{
                new RecordsSerializer.Column("id", RecordsSerializer.ColumnType.INT),
                new RecordsSerializer.Column("value", RecordsSerializer.ColumnType.STRING)
        });
        RecordStorage recordStorage = new RecordStorage(bufferPool, recordsSerializer, walManager, index, pageManager);
        executor = new Executor(txnManager, walManager, lockManager, recordStorage);
    }

    @Test
    public void testInsertAndSelect() throws Exception {
        executeSql("INSERT INTO kv (id, value) VALUES (1, 'world')");
        Result result = executeSql("SELECT * FROM kv WHERE id = 1");
        assertTrue(result.ok);
        assertEquals(1, result.rows.size());
        assertEquals("1", result.rows.get(0).key);
        assertEquals("world", result.rows.get(0).value);
    }

    @Test
    public void testDelete() throws Exception {
        executeSql("INSERT INTO kv (id, value) VALUES (1, 'world')");
        executeSql("DELETE FROM kv WHERE id = 1");
        Result result = executeSql("SELECT * FROM kv WHERE id = 1");
        assertTrue(result.ok);
        assertTrue(result.rows.isEmpty());
    }

    // @Test
    // public void testBetween() throws Exception {
    //     executeSql("INSERT INTO kv (id, value) VALUES (1, '1')");
    //     executeSql("INSERT INTO kv (id, value) VALUES (2, '2')");
    //     executeSql("INSERT INTO kv (id, value) VALUES (3, '3')");
    //     Result result = executeSql("SELECT * FROM kv WHERE id BETWEEN 1 AND 2");
    //     assertTrue(result.ok);
    //     assertEquals(2, result.rows.size());
    // }

    private Result executeSql(String sql) throws Exception {
        Tokenizer tokenizer = new Tokenizer(sql);
        List<Token> tokens = tokenizer.tokenize();
        Parser parser = new Parser(tokens);
        return executor.execute(parser.parse());
    }
}
