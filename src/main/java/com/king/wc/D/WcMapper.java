package com.king.wc.D;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-23 17:16
 */
public class WcMapper extends Mapper<Object, Text, NewKey, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\t");
        NewKey nk = new NewKey(Long.parseLong(strings[1]), Long.parseLong(strings[2]));
        context.write(nk ,new Text(strings[0]) );
        System.out.println(Arrays.toString(strings));
        //context.write();
    }
}
