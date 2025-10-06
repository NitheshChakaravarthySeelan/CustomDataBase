package com.minidb.sql.parser.ast;

public abstract class Predicate implements java.io.Serializable {
    private final String column;
    public Predicate(String column) { this.column = column; }
    public String getColumn() { return column; }
}

class EqualsPredicate extends Predicate {
    private final String value;
    public EqualsPredicate(String column, String value) {
        super(column);
        this.value = value;
    }
    public String getValue() { return value; }
    @Override
    public String toString() {
        return getColumn() + " = '" + value + "'";
    }
}

class BetweenPredicate extends Predicate {
    private final String low, high;
    public BetweenPredicate(String column, String low, String high) {
        super(column);
        this.low = low;
        this.high = high;
    }
    public String getLow() { return low; }
    public String getHigh() { return high; }
    @Override
    public String toString() {
        return getColumn() + " BETWEEN '" + low + "' AND '" + high + "'";
    }
}

