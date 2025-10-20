package com.minidb.sql.parser.ast;

public class InsertCommand implements Command {
    private final String tableName;
    private final String keyLiteral;
    private final String valueLiteral;

    public InsertCommand(String tableName, String keyLiteral, String valueLiteral) {
        this.tableName = tableName;
        this.keyLiteral = keyLiteral;
        this.valueLiteral = valueLiteral;
    }

    public String getTableName() {
        return tableName;
    }

    public String getKeyLiteral() {
        return keyLiteral;
    }

    public String getValueLiteral() {
        return valueLiteral;
    }

public String toString() {
        return "InsertCommand{" +
                "tableName='" + tableName + '\'' +
                ", keyLiteral='" + keyLiteral + '\'' +
                ", valueLiteral='" + valueLiteral + '\'' +
                '}';
    }
}