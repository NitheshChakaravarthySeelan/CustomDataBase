package com.minidb.sql.parser.ast;

public class SelectCommand implements Command {
    private final String tableName;
    private final String column;
    private final Predicate predicate;

    public SelectCommand(String tableName, String column, Predicate predicate) {
        this.tableName = tableName;
        this.column = column;
        this.predicate = predicate;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumn() {
        return column;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "SelectCommand{"
                + "tableName='" + tableName + "'"
                + ", column='" + column + "'"
                + ", predicate=" + predicate
                + "}";
    }
}