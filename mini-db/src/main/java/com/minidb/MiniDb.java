package com.minidb;

import com.minidb.index.BPlusTree;
import com.minidb.index.Serializer;
import com.minidb.log.RecoveryManager;
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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class MiniDb {

    public static void main(String[] args) throws IOException {
        File dbDir = new File("minidb_data");
        if (!dbDir.exists()) {
            dbDir.mkdir();
        }

        // Initialize managers and index
        PageManager pageManager = new PageManager(new File(dbDir, "minidb.db").getPath(), 4096);
        BufferPool bufferPool = new BufferPool(pageManager, 10);
        WALManager walManager = new WALManager(dbDir);
        Serializer<String> serializer = new StringSerializer();
        BPlusTree<String, String> index = new BPlusTree<>(5, serializer, serializer);
        LockManager lockManager = new LockManager();
        TxnManager txnManager = new TxnManager(lockManager, walManager);

        // Recover from WAL
        // RecoveryManager recoveryManager = new RecoveryManager(walManager, null);
        // recoveryManager.recover();

        Executor executor = new Executor(txnManager, walManager, index, lockManager, serializer);

        System.out.println("MiniDB SQL REPL. Enter .exit to quit.");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            if (".exit".equalsIgnoreCase(line)) {
                break;
            }
            if (line.trim().isEmpty()) {
                continue;
            }

            try {
                Tokenizer tokenizer = new Tokenizer(line);
                List<Token> tokens = tokenizer.tokenize();
                Parser parser = new Parser(tokens);
                Result result = executor.execute(parser.parse());
                if (result.ok) {
                    if (result.rows != null && !result.rows.isEmpty()) {
                        result.rows.forEach(row -> System.out.println(row.key + " -> " + row.value));
                    } else {
                        System.out.println("OK");
                    }
                } else {
                    System.err.println("Error: " + result.errorMessage);
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        // Clean up
        System.out.println("\nFlushing pages and closing DB...");
        bufferPool.flushAllPages();
        walManager.close();
        pageManager.close();
        System.out.println("DB closed.");
    }
}