package com.minidb.sql.parser.ast;

public class BetweenPredicate extends Predicate {
    private final String low, high;

    public BetweenPredicate(String column, String low, String high) {
        super(column);
        this.low = low;
        this.high = high;
    }

    public String getLow() {
        return low;
    }

    public String getHigh() {
        return high;
    }

    @Override
    public String toString() {
        return getColumn() + " BETWEEN '" + low + "' AND '" + high + "'";
    }
}
