package com.king.wc.E;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 19:31
 */
public class AllCityMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static final String LABEL = "a";
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cityName = value.toString();
        context.write(new Text(cityName),new Text(LABEL + cityName));
    }
}
