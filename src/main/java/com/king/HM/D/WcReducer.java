package com.king.HM.D;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-23 17:19
 */
public class WcReducer extends Reducer<NewKey, Text, Text, Text> {
    public WcReducer() {
    }

    @Override
    protected void reduce(NewKey nk, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {
        Iterator<Text> its = iterable.iterator();
        while (its.hasNext()) {
            Text name = its.next();
            context.write(name, new Text(nk.toString()));
        }
    }
}
