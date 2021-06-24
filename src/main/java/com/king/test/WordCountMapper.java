package com.king.test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-22 17:01
 */
import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}