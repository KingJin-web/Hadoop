package com.king.wc.H.p2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-26 10:07
 */
public class CountReduce extends Reducer<IntWritable, LongWritable, IntWritable, Text> {
    //车辆总和
    private Long total = 0L;
    private Map<Integer, Long> map = new HashMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum = 0L;
        for (LongWritable v : values) {
            sum += v.get();

        }
        total += sum;
        map.put(key.get(), sum);
    }

    DoubleWritable num = new DoubleWritable();

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        TreeSet<Integer> keys = new TreeSet(map.keySet());
        for (Integer key : keys) {
            Long value = map.get(key);
            double percent = value / (double) total;
            num.set(percent);

            context.write(new IntWritable(key), new Text(value + "\t" + num));


        }

    }
}
