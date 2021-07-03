package com.king.D0703.B;
import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
/**
 * @program: hdfs
 * @description: c. 统计涉嫌养号的手机.
 *        一个手机号对应了多个sim卡号.
 * @author: King
 * @create: 2021-07-03 19:36
 */
public class JZ3 {


    /**
     POS:
     IMSI(在所有蜂窝网络中不重复的识别码) IMEI(移动设备识别码)   UPDATETYPE    LOC(基站)        TIME
     0000000000	                   0054775807	               0	       00000033	  2021-07-01 13:26:21
     */
    public static class JZ3Mapper extends Mapper<Object, Text, Text, Text> {
        private IntWritable one=new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []strings=value.toString().split("\t");
            context.write(new Text(strings[1]),new Text(strings[0]));
        }
    }

    /**
     *   c. 统计涉嫌养号的手机.
     *              一个手机号对应了多个sim卡号.
     */
    public static class JZ3Reducer extends Reducer<Text,Text,Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator it=values.iterator();
            if (it.hasNext()){
                context.write(key,new Text("涉嫌养号"));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length < 1) {
            args = new String[4];
            args[0] = "D:\\wordcount\\input\\D0701";
            args[1] = "D:\\wordcount\\output\\D0701";
            args[2] = "2021-07-01";
            args[3] = "07-09-17-24";
        }
        Configuration conf=new Configuration();
        Path inputPath=new Path(args[0]);
        Path outputPath=new Path(args[1]);
        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        Job job=new Job(conf,"养号嫌疑");
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setJarByClass(JZ3.class);

        job.setMapperClass(JZ3Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.setReducerClass(JZ3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);

        ReadOutput.read(args[1]);
    }
}
