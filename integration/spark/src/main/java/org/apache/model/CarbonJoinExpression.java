package org.apache.model;

public class CarbonJoinExpression {

    //This class will store the joinExpression
    TmpColumn leftCol;
    TmpColumn rightCol;
    String operator;

    public CarbonJoinExpression() {

    }

    public CarbonJoinExpression(TmpColumn leftCol, TmpColumn rightCol, String operator) {
        this.leftCol = leftCol;
        this.rightCol = rightCol;
        this.operator = operator;
    }

    public TmpColumn getLeftCol() {
        return leftCol;
    }

    public void setLeftCol(TmpColumn leftCol) {
        this.leftCol = leftCol;
    }

    public TmpColumn getRightCol() {
        return rightCol;
    }

    public void setRightCol(TmpColumn rightCol) {
        this.rightCol = rightCol;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}

