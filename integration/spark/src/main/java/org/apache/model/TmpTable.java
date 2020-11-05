package org.apache.model;

public class TmpTable {

    String database;
    String table;

    public TmpTable() {

    }

    public TmpTable(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
