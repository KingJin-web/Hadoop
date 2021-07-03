package com.king.D0703.A;

import com.king.util.ReadOutput;
import com.king.weblog.KPI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-03 14:09
 */
public class Log1 {

    //194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
    public static class SpiderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outKey = new Text();
        IntWritable outValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.parser(value.toString());

            if (kpi.isValid()) {
                System.out.println(kpi.getHttp_user_agent());
                if (kpi.getHttp_user_agent().contains("spider") || kpi.getHttp_user_agent().contains("bot")) {
                    outKey.set("爬虫");
                    context.write(outKey, outValue);
                } else {
                    outKey.set("非爬虫");
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class SpiderReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private Text outputKey = new Text();
        private DoubleWritable outputValue = new DoubleWritable(0);
        private Map<String, Integer> map = new HashMap<>();

        float count = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable v : values) {
                num += v.get();
            }
            map.put(key.toString(), num);
            System.out.println(num);
            count += num;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
               // System.out.println("key = " + entry.getKey() + ", value = " + entry.getValue());
                outputKey.set(entry.getKey());
                outputValue.set(entry.getValue() / count);
               // System.out.println(entry);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        Configuration conf = new Configuration();
        Path inputPath = new Path("D:\\wordcount\\input\\data\\access.log.10");
        Path outputPath = new Path("D:\\wordcount\\output\\data");
        //创建  输出 文件 ，并判断 这个输出 文件是否存在
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "爬虫数据统计");

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(Log1.class);

        job.setMapperClass(SpiderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SpiderReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.waitForCompletion(true);
        ReadOutput.readAll(outputPath);
    }
}
