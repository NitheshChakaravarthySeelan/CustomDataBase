package com.minidb;

import com.minidb.sql.executor.Executor;
import com.minidb.sql.executor.Result;
import com.minidb.sql.parser.Parser;
import com.minidb.sql.parser.Token;
import com.minidb.sql.parser.Tokenizer;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class MiniDbRepl {

    private final Executor executor;
    private final String nodeId;

    public MiniDbRepl(Executor executor, String nodeId) {
        this.executor = executor;
        this.nodeId = nodeId;
    }

    public void start() {
        System.out.println("MiniDB SQL REPL for node " + nodeId + ". Enter .exit to quit.");
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
            }
            catch (RuntimeException e) {
                System.err.println("Runtime Error: " + e.getMessage());
            }
            catch (Exception e) {
                System.err.println("Unexpected Error: " + e.getMessage());
            }
        }
    }
}
