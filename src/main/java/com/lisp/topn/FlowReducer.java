package com.lisp.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReducer extends Reducer<FlowBean,Text,Text,FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> value = values.iterator();
        for(int i = 0; i<10;i++){
            if(value.hasNext()){
                context.write(value.next(),key);
            }
        }
    }
}
