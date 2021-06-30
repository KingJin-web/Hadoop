package com.king.weblog.A;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-27 16:56
 */
public class A_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outputKey = new Text();
    private static final IntWritable outputValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将这一行转化为string
        String lines = value.toString();
        //以空格切分
        String[] line = lines.split(" ");
        //获得ip
        String ip = line[0];

        // 所以在context里面写的内容就是 key：ip ，value 是1
        context.write(new Text(ip), outputValue);

    }
}
