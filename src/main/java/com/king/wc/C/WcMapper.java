package com.king.wc.C;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-23 17:16
 */
public class WcMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(" ");
        System.out.println(Arrays.toString(strings));
        //context.write();
    }
}
