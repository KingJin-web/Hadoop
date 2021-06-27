package com.king.wc.H.p2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: hdfs
 * @description: 映射输出每个月的销售数量    月份   销量
 * @author: King
 * @create: 2021-06-26 10:07
 */
public class CountMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    int count = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] ss = value.toString().split(",");
        if (ss.length > 11 && ss[11] != null && ss[1] != null && !"".equals(ss[1]) ) {
            String dates = ss[1];
            int month = Integer.parseInt(dates);
            long num = Long.parseLong(ss[11]);

            context.write(new IntWritable(month), new LongWritable(num));
        }

    }
}
