package org.apache.model;

public class CarbonJoinExpression {

    String srcTable;
    String srcCol;
    String tgTable;
    String tgCol;

    public CarbonJoinExpression(String srcTable, String srcCol, String tgTable, String tgCol) {
        this.srcTable = srcTable;
        this.srcCol = srcCol;
        this.tgTable = tgTable;
        this.tgCol = tgCol;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getSrcCol() {
        return srcCol;
    }

    public void setSrcCol(String srcCol) {
        this.srcCol = srcCol;
    }

    public String getTgTable() {
        return tgTable;
    }

    public void setTgTable(String tgTable) {
        this.tgTable = tgTable;
    }

    public String getTgCol() {
        return tgCol;
    }

    public void setTgCol(String tgCol) {
        this.tgCol = tgCol;
    }
}

