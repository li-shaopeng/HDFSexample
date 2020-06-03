package com.lisp.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlowComparator extends WritableComparator {
    protected FlowComparator() {
        super(FlowBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
