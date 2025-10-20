package com.minidb.sql.parser.ast;

public abstract class Predicate {
    private final String column;

    public Predicate(String column) {
        this.column = column;
    }

    public String getColumn() {
        return column;
    }
}