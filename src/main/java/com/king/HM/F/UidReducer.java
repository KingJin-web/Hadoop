package com.king.HM.F;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 20:41
 */
public class UidReducer extends Reducer<Text, IntWritable, Text, Text> {
    public UidReducer() {
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
           sum += val.get();
        }

        //context.write();
    }
}
