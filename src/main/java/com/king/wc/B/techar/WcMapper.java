package com.king.wc.B.techar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-23 17:08
 */

public class WcMapper extends Mapper<Object, Text, IntWritable, IntWritable> {


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int num = Integer.parseInt(value.toString());
        IntWritable intWritable = new IntWritable(num);
        context.write(intWritable, new IntWritable(1));
    }
}
