package com.minidb.sql.parser.ast;

public class DeleteCommand implements Command {
    private final String tableName;
    private final Predicate predicate;

    public DeleteCommand(String tableName, Predicate predicate) {
        this.tableName = tableName;
        this.predicate = predicate;
    }

    public String getTableName() {
        return tableName;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "DeleteCommand{"
                + "tableName='" + tableName + "'" + 
                ", predicate=" + predicate + 
                '}';
    }
}