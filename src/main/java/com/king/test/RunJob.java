package com.king.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-22 17:03
 */
public class RunJob {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(RunJob.class);
        job.setJobName("wordCount");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("D:\\wordcount\\input"));
        Path outPath = new Path("D:\\wordcount\\output");
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        boolean completion = job.waitForCompletion(true);
        if (completion) {
            System.out.println("执行完成");
        }


    }
}