package com.king.HM.H.p1_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-26 10:07
 */
public class CountReduce extends Reducer<IntWritable, LongWritable,IntWritable,LongWritable> {
    //车辆总和
    private Long total = 0L;
    private Map<String, Long> map = new HashMap<>();
    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum = 0L;
        for (LongWritable v : values){
            sum += v.get();

        }
    }
}
