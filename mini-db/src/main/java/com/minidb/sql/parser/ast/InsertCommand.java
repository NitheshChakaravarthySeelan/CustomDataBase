package com.minidb.sql.parser.ast;

public class InsertCommand implements Command{
    public final String table;
    public final String key;
    public final String value;


    public InsertCommand(String table, String key, String value) {
        this.table = table;
        this.key = key;
        this.value = value;
    }

    public String getTableName() {
        return table;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;}

    @Override
    public String toString() {
        return "InsertCommand{" +
                "table='" + table + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }  
}
