package com.minidb.sql.parser.ast;

public class SelectCommand implements Command {
    public final String table;
    public final String column;
    public final String predicateColumn;
    public final String predicateValue;

    public SelectCommand(String table, String column, String predicateColumn, String predicateValue) {
        this.table = table;
        this.column = column;
        this.predicateColumn = predicateColumn;
        this.predicateValue = predicateValue;
    }

    public String getTableString() {
        return table;
    }

    public String getColumnString() {
        return column;
    }   

    public Predicate getPredicate() {
        return new EqualsPredicate(predicateColumn, predicateValue);
    }

    @Override
    public String toString() {  
        return "SelectCommand{" +
                "table='" + table + '\'' +
                ", column='" + column + '\'' +
                ", predicateColumn='" + predicateColumn + '\'' +
                ", predicateValue='" + predicateValue + '\'' +
                '}';
    }
}
