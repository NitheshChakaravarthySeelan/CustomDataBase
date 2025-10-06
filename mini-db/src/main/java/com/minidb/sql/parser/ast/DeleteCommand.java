package com.minidb.sql.parser.ast;

public class DeleteCommand implements Command {
    private final String tableName;
    private final String keyLiteral;

    public DeleteCommand(String tableName, String keyLiteral) {
        this.tableName = tableName;
        this.keyLiteral = keyLiteral;
    }

    public String getTableName() { return tableName; }
    public String getKeyLiteral() { return keyLiteral; }

    @Override
    public String toString() {
        return "DeleteCommand{" +
                "tableName='" + tableName + '\'' +
                ", key='" + keyLiteral + '\'' +
                '}';
    }
}
