package com.king.wc.H.p3;

import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-26 18:52
 */
public class CityApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new CityApp(), args);

        System.exit(i);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Path in1 = new Path("D:\\wordcount\\input\\H");
        Path in2 = new Path("D:\\wordcount\\output\\H\\part-r-00000");
        Path outPath1 = new Path("D:\\wordcount\\output\\H");
        Path outPath2 = new Path("D:\\wordcount\\output\\H\\A");
        if (strings.length >= 1) {
            in1 = new Path(strings[0]);
            in2 = new Path(strings[1]);
            outPath1 = new Path(strings[2]);
            outPath2 = new Path(strings[3]);
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job1 = Job.getInstance(super.getConf(), "各个市,区的汽车销售情况");
        job1.setJarByClass(CityApp.class);

        FileInputFormat.addInputPath(job1, in1);
        FileOutputFormat.setOutputPath(job1, outPath1);
        job1.setMapperClass(CityCountMapper.class);
        job1.setReducerClass(CityCountReduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        if (fs.exists(outPath1)) {
            fs.delete(outPath1, true);
        }

        job1.waitForCompletion(true);
        Job job2 = Job.getInstance(super.getConf(), "各个市的汽车销售情况");
        job2.setJarByClass(CityApp.class);
        FileInputFormat.addInputPath(job2, in2);
        FileOutputFormat.setOutputPath(job2, outPath2);
        job2.setMapperClass(CityMapper2.class);
        job2.setReducerClass(CityReduce2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        // Thread.sleep(100000);
        job2.waitForCompletion(true);

        ReadOutput.read(outPath1);
        ReadOutput.read(outPath2);
        return 0;
    }
}
