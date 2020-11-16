package org.apache.model;

public class TmpColumn {

    private String table;
    private String colName;

    public TmpColumn() {

    }

    public TmpColumn(String table, String colName) {
        this.table = table;
        this.colName = colName;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }
}
