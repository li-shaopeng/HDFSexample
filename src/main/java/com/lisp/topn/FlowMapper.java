package com.lisp.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,FlowBean,Text> {
    private FlowBean fb = new FlowBean();
    private Text phone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        phone.set(words[0]);
        fb.set(Long.parseLong(words[1]), Long.parseLong(words[2]));
        context.write(fb, phone);
    }
}
