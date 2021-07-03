package com.king.D0703.A;

import com.king.util.ReadOutput;
import com.king.weblog.KPI;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: hdfs
 * @description: 统计用户来自的地域（各省、直辖市、自治区，国外），计算各地域访问占的百分比, 使用纯真ip数据库
 * @author: King
 * @create: 2021-07-03 15:50
 */
public class Log3 extends Configured implements Tool {
    public static class Log3PvMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private static final IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String browser = KPI.GetAddress(value.toString());
            //System.out.println(browser);
            outputKey.set(browser);
            context.write(outputKey, outputValue);


        }


    }

    public static class Log3PvReducer extends Reducer<Text, IntWritable, Text,  Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        float num = 0;
        Map<String, Float> map = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //统计单词数
            float count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            num += count;
            map.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // outputValue.set(count / num);
            for (Map.Entry<String, Float> entry : map.entrySet()) {
                // System.out.println("key = " + entry.getKey() + ", value = " + entry.getValue());
                outputKey.set(entry.getKey());
                outputValue.set((entry.getValue() / num * 100) + " %");

                context.write(outputKey, outputValue);
            }

        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path("D:\\wordcount\\input\\data\\access.log.10");
        Path outPath = new Path("D:\\wordcount\\output\\data");
        Job job = Job.getInstance(super.getConf(), "统计浏览器数量");
        job.setJarByClass(Log3.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(Log3PvMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Log3PvReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass( Text.class);


        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean b = job.waitForCompletion(true);
        new ReadOutput(outPath);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //  BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new Log3(), args);

        System.exit(i);
    }
}
