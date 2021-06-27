package com.king.wc.H.p3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-26 11:34
 */
public class CityCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(",");
        if (strs.length == 39 && strs[1].equals("4")) {
            String city = strs[2] + "," + strs[3];
            context.write(new Text(city), new LongWritable(Long.parseLong(strs[11])));
        }
    }


}
