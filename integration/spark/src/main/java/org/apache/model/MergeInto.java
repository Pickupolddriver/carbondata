package org.apache.model;

public class MergeInto {
    TmpTable target;
    TmpTable source;

    public MergeInto(TmpTable target, TmpTable source) {
        this.target = target;
        this.source = source;
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
}
