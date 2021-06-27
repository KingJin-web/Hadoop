package com.king.wc.G;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 20:55
 */
public class WeatherPartition extends Partitioner<Weather, IntWritable> {
    public WeatherPartition() {
        System.out.println(WeatherPartition.class.getSimpleName());
    }

    @Override
    public int getPartition(Weather weather, IntWritable intWritable, int i) {
        System.out.println( weather );
        System.out.println(intWritable);
        System.out.println(i);
        int result = ( weather.getYear() - 1949 ) % i;
        System.out.println(weather.toString() + "被分配在：" + result);
        return result;
    }

}
