package com.minidb;

import com.minidb.index.BPlusTree;
import com.minidb.log.WALManager;
import com.minidb.serializers.StringSerializer;
import com.minidb.sql.executor.Executor;
import com.minidb.sql.executor.Result;
import com.minidb.sql.parser.Parser;
import com.minidb.sql.parser.Token;
import com.minidb.sql.parser.Tokenizer;
import com.minidb.storage.BufferPool;
import com.minidb.storage.PageManager;
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
    private BPlusTree<String, String> bpt;

    @Before
    public void setup() throws IOException {
        File dbDir = new File("minidb_test_data");
        if (!dbDir.exists()) {
            dbDir.mkdir();
        }
        PageManager pageManager = new PageManager(new File(dbDir, "minidb.db").getPath(), 4096);
        BufferPool bufferPool = new BufferPool(pageManager, 10);
        WALManager walManager = new WALManager(dbDir);
        StringSerializer serializer = new StringSerializer();
        bpt = new BPlusTree<>(5, serializer, serializer);
        LockManager lockManager = new LockManager();
        TxnManager txnManager = new TxnManager(lockManager, walManager);
        executor = new Executor(txnManager, walManager, bpt, lockManager, serializer);
    }

    @Test
    public void testInsertAndSelect() throws Exception {
        executeSql("INSERT INTO kv (key, value) VALUES ('hello', 'world')");
        Result result = executeSql("SELECT * FROM kv WHERE key = 'hello'");
        assertTrue(result.ok);
        assertEquals(1, result.rows.size());
        assertEquals("hello", result.rows.get(0).key);
        assertEquals("world", result.rows.get(0).value);
    }

    @Test
    public void testDelete() throws Exception {
        executeSql("INSERT INTO kv (key, value) VALUES ('hello', 'world')");
        executeSql("DELETE FROM kv WHERE key = 'hello'");
        Result result = executeSql("SELECT * FROM kv WHERE key = 'hello'");
        assertTrue(result.ok);
        assertTrue(result.rows.isEmpty());
    }

    @Test
    public void testBetween() throws Exception {
        executeSql("INSERT INTO kv (key, value) VALUES ('a', '1')");
        executeSql("INSERT INTO kv (key, value) VALUES ('b', '2')");
        executeSql("INSERT INTO kv (key, value) VALUES ('c', '3')");
        Result result = executeSql("SELECT * FROM kv WHERE key BETWEEN 'a' AND 'b'");
        assertTrue(result.ok);
        assertEquals(2, result.rows.size());
    }

    private Result executeSql(String sql) throws Exception {
        Tokenizer tokenizer = new Tokenizer(sql);
        List<Token> tokens = tokenizer.tokenize();
        Parser parser = new Parser(tokens);
        return executor.execute(parser.parse());
    }
}
