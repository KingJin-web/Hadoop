package com.king.homework.A;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-27 18:41
 */
public class A_Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {
    private LongWritable count = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int c = 0;
        for (IntWritable i : values) {
            c += i.get();
        }
        count.set(c);
        context.write(key, count);
    }
}
