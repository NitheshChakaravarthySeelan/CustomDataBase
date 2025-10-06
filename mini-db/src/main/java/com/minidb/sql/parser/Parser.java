package com.minidb.sql.parser;

import java.util.List;

// To check if the syntax is correct
public class Parser {
    private final List<Token> tokens;
    private int pos = 0;

    public Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    private Token peek() {
        return tokens.get(pos);
    }

    private Token consume() {
        return tokens.get(pos++);
    }

    private void expect(String val) {
        Token t = consume();
        if (!t.value.equalsIgnoreCase(val)) {
            throw new RuntimeException("Expected " + val + " but got " + t.value + " at " + t.position);
        }
    }

    public Command parseStatement() {
        Token t = peek();
        if (t.value.equals("INSERT")) return parseInsert();
        if (t.value.equals("SELECT")) return parseSelect();
        if (t.value.equals("DELETE")) return parseDelete();
        throw new RuntimeException("Unknown statement at pos " + t.position);
    }

    private InsertCommand parseInsert() {
        expect("INSERT");
        expect("INTO");
        String table = consume().value;
        expect("(");
        String col1 = consume().value;
        expect(",");
        String col2 = consume().value;
        expect(")");
        expect("VALUES");
        expect("(");
        String key = consume().value.replaceAll("'", "");
        expect(",");
        String value = consume().value.replaceAll("'", "");
        expect(")");
        return new InsertCommand(table, key, value);
    }

    private SelectCommand parseSelect() {
        expect("SELECT");
        String column = consume().value;
        expect("FROM");
        String table = consume().value;
        expect("WHERE");
        String predCol = consume().value;
        expect("=");
        String predVal = consume().value.replaceAll("'", "");
        return new SelectCommand(table, column, predCol, predVal);
    }

    private DeleteCommand parseDelete() {
        expect("DELETE");
        expect("FROM");
        String table = consume().value;
        expect("WHERE");
        String col = consume().value;
        expect("=");
        String val = consume().value.replaceAll("'", "");
        return new DeleteCommand(table, col, val);
    }
}
