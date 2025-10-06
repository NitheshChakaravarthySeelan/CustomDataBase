package com.minidb.sql.parser;

public class ParseException {
    public ParseException(String message) {
        throw new RuntimeException(message);
    }
}

