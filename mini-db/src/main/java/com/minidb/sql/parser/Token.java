package com.minidb.sql.parser;

import com.minidb.sql.parser.Tokenizer.TokenType;

public class Token {
    public final TokenType type;
    public final String value;
    public final int position;

    public Token(TokenType type, String value, int position) {
        this.type = type;
        this.value = value;
        this.position = position;
    }

    @Override
    public String toString() {
        return type + "(" + value + ")";
    }
}