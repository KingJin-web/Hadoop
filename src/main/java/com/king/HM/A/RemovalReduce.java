package com.king.HM.A;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RemovalReduce extends Reducer<Text, NullWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("key: " + key + " value: " + values.toString());
        context.write(key,NullWritable.get());
    }
}
