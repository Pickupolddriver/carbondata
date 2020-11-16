package org.apache.model;

import java.util.List;

public class TmpTable extends BaseClass {

    String database;
    String table;
    String alias;
    List<TmpColumn> columns;

    public TmpTable() {

    }

    public TmpTable(String database, String table, String alias) {
        this.database = database;
        this.table = table;
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
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
