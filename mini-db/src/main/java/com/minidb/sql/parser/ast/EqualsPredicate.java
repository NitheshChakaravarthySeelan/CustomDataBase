package com.minidb.sql.parser.ast;

public class EqualsPredicate extends Predicate {
    private final String value;

    public EqualsPredicate(String column, String value) {
        super(column);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return getColumn() + " = '" + value + "'";
    }
}
