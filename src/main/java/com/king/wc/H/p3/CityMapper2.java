package com.king.wc.H.p3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-26 11:59
 */
public class CityMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split("\t");
        if (strs != null && strs.length > 1) {
            //
            System.out.println(Arrays.toString(strs));
            String[] city = strs[0].split(",");
            if (city != null && city.length > 0) {
                context.write(new Text(city[0]), new LongWritable(Long.parseLong(strs[1])));
            }
        }

    }
}
