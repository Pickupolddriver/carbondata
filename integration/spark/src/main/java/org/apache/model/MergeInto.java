package org.apache.model;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.command.mutation.merge.MergeDataSetMatches;

public class MergeInto {
    TmpTable target;
    TmpTable source;
    Expression mergeCondition;
    MergeDataSetMatches mergeDataSetMatches;

    public MergeInto(TmpTable target, TmpTable source, Expression mergeCondition) {
        this.target = target;
        this.source = source;
        this.mergeCondition = mergeCondition;
    }

    public TmpTable getTarget() {
        return target;
    }

    public void setTarget(TmpTable target) {
        this.target = target;
    }

    public TmpTable getSource() {
        return source;
    }

    public void setSource(TmpTable source) {
        this.source = source;
    }

    public Expression getMergeCondition() {
        return mergeCondition;
    }

    public void setMergeCondition(Expression mergeCondition) {
        this.mergeCondition = mergeCondition;
    }

    public MergeDataSetMatches getMergeDataSetMatches() {
        return mergeDataSetMatches;
    }

    public void setMergeDataSetMatches(MergeDataSetMatches mergeDataSetMatches) {
        this.mergeDataSetMatches = mergeDataSetMatches;
    }
}
