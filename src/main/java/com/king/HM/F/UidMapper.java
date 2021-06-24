package com.king.HM.F;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 20:35
 */
public class UidMapper extends Mapper<Object, Text, Text, IntWritable> {
    public UidMapper() {
        System.out.println("UidMapper");
    }

    public static final IntWritable ONE = new IntWritable(1);
    private Text uidText = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("mapper");
        String line = value.toString();
        String[] arr = line.split("\t");
        if (null != arr && arr.length == 6) {
            String uid = arr[1];
            if (null != uid && "".equals(uid.trim())) {
                uidText.set(uid);
                context.write(uidText, ONE);
            }
        }
    }
}
