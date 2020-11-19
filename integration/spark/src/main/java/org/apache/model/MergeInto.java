package org.apache.model;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.command.mutation.merge.MergeAction;

import java.util.List;

public class MergeInto {
    TmpTable target;
    TmpTable source;
    Expression mergeCondition;
    List<Expression> mergeExpressions;
    List<MergeAction> mergeActions;

    public MergeInto(TmpTable target, TmpTable source, Expression mergeCondition, List<Expression> mergeExpressions, List<MergeAction> mergeActions) {
        this.target = target;
        this.source = source;
        this.mergeCondition = mergeCondition;
        this.mergeExpressions = mergeExpressions;
        this.mergeActions = mergeActions;
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

    public List<Expression> getMergeExpressions() {
        return mergeExpressions;
    }

    public void setMergeExpressions(List<Expression> mergeExpressions) {
        this.mergeExpressions = mergeExpressions;
    }

    public List<MergeAction> getMergeActions() {
        return mergeActions;
    }

    public void setMergeActions(List<MergeAction> mergeActions) {
        this.mergeActions = mergeActions;
    }
}
