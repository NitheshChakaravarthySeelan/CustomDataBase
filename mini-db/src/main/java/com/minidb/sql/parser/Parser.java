package com.minidb.sql.parser;

import com.minidb.sql.parser.ast.*;
import com.minidb.sql.parser.Tokenizer.TokenType;

import java.util.List;

public class Parser {
    private final List<Token> tokens;
    private int pos = 0;

    public Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    public Command parse() throws ParseException {
        Command cmd = parseStatement();
        if (peek().type != TokenType.EOF) {
            throw new ParseException("Extra characters at end of statement");
        }
        return cmd;
    }

    private Command parseStatement() throws ParseException {
        Token t = peek();
        if (t.type != TokenType.KEYWORD) {
            throw new ParseException("Expected a command but got " + t.value);
        }
        switch (t.value) {
            case "INSERT":
                return parseInsert();
            case "SELECT":
                return parseSelect();
            case "DELETE":
                return parseDelete();
            default:
                throw new ParseException("Unknown command: " + t.value);
        }
    }

    private InsertCommand parseInsert() throws ParseException {
        expectKeyword("INSERT");
        expectKeyword("INTO");
        String tableName = expectIdentifier();
        expectSymbol("(");
        expectIdentifier(); // We are ignoring column names for now
        expectSymbol(",");
        expectIdentifier();
        expectSymbol(")");
        expectKeyword("VALUES");
        expectSymbol("(");
        String key = expectLiteral();
        expectSymbol(",");
        String value = expectStringLiteral();
        expectSymbol(")");
        return new InsertCommand(tableName, key, value);
    }

    private SelectCommand parseSelect() throws ParseException {
        expectKeyword("SELECT");
        String column = expectIdentifierOrStar();
        expectKeyword("FROM");
        String tableName = expectIdentifier();
        Predicate predicate = null;
        if (matchKeyword("WHERE")) {
            predicate = parsePredicate();
        }
        return new SelectCommand(tableName, column, predicate);
    }

    private DeleteCommand parseDelete() throws ParseException {
        expectKeyword("DELETE");
        expectKeyword("FROM");
        String tableName = expectIdentifier();
        expectKeyword("WHERE");
        Predicate predicate = parsePredicate();
        return new DeleteCommand(tableName, predicate);
    }

    private Predicate parsePredicate() throws ParseException {
        String column = expectIdentifier();
        if (matchKeyword("BETWEEN")) {
            String low = expectLiteral();
            expectKeyword("AND");
            String high = expectLiteral();
            return new BetweenPredicate(column, low, high);
        } else {
            expectSymbol("=");
            String value = expectLiteral();
            return new EqualsPredicate(column, value);
        }
    }

    private Token peek() {
        return tokens.get(pos);
    }

    private Token consume() {
        return tokens.get(pos++);
    }

    private void expectKeyword(String keyword) throws ParseException {
        Token t = consume();
        if (t.type != TokenType.KEYWORD || !t.value.equalsIgnoreCase(keyword)) {
            throw new ParseException("Expected keyword '" + keyword + "' but got " + t.value);
        }
    }

    private String expectIdentifier() throws ParseException {
        Token t = consume();
        if (t.type != TokenType.IDENTIFIER) {
            throw new ParseException("Expected an identifier but got " + t.value);
        }
        return t.value;
    }

    private String expectIdentifierOrStar() throws ParseException {
        Token t = consume();
        if (t.type != TokenType.IDENTIFIER && (t.type != TokenType.SYMBOL || !t.value.equals("*"))) {
            throw new ParseException("Expected an identifier or ' * ' but got " + t.value);
        }
        return t.value;
    }

    private String expectStringLiteral() throws ParseException {
        Token t = consume();
        if (t.type != TokenType.STRING_LITERAL) {
            throw new ParseException("Expected a string literal but got " + t.value);
        }
        return t.value;
    }

    private String expectLiteral() throws ParseException {
        Token t = consume();
        if (t.type != TokenType.STRING_LITERAL && t.type != TokenType.NUMERIC_LITERAL) {
            throw new ParseException("Expected a string or numeric literal but got " + t.value);
        }
        return t.value;
    }

    private void expectSymbol(String symbol) throws ParseException {
        Token t = consume();
        if (t.type != TokenType.SYMBOL || !t.value.equals(symbol)) {
            throw new ParseException("Expected symbol '" + symbol + "' but got " + t.value);
        }
    }

    private boolean matchKeyword(String keyword) {
        if (peek().type == TokenType.KEYWORD && peek().value.equalsIgnoreCase(keyword)) {
            pos++;
            return true;
        }
        return false;
    }
}