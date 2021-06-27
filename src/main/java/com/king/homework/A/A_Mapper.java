package com.king.homework.A;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-27 16:56
 */
public class A_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String[] arr =  value.toString().split(" ");
       context.write(new Text(Arrays.toString(arr)),new IntWritable(1));

    }
}
