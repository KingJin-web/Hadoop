package com.king.HM.E;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 19:31
 */
public class SomeCityMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static final String LABEL = "s_";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split("\t");
        String cityName = s[0];
        context.write(new Text(cityName), new Text(LABEL + value.toString()));
    }
}
