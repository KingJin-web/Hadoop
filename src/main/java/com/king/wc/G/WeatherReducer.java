package com.king.wc.G;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 20:55
 */
public class WeatherReducer extends
        Reducer<Weather, IntWritable, Text, NullWritable> {
    public WeatherReducer() {
        System.out.println("WeatherReducer");
    }

    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable i : values) {
            count++;
            if (count > 2) {
                break;
            }
            System.out.println(key);
            String result = key.getYear() + "-" + key.getMonth() +
                    "-" + key.getMonth() + "-" + i.get();
            context.write(new Text(result),NullWritable.get());

        }
    }
}