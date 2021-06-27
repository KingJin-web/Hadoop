package com.king.wc.H.p2;

import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @description: 山西省2013年每个月的汽车销售数量的比例
 * @author: King
 * @create: 2021-06-26 10:06
 */
public class CountApp extends Configured implements Tool {
        public static void main(String[] args) throws Exception {
            BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
            int i = ToolRunner.run(new CountApp(), args);

            System.exit(i);
        }

        @Override
        public int run(String[] strings) throws Exception {
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(conf);
            Path inPath = new Path("D:\\wordcount\\input\\H\\cars.txt");
            Path outPath = new Path("D:\\wordcount\\output\\H");
            Job job = Job.getInstance(super.getConf(),"统计乘用车辆和商用车辆的数量和销售额分布");
            job.setJarByClass(CountApp.class);

            FileInputFormat.addInputPath(job,inPath);
            FileOutputFormat.setOutputPath(job,outPath);

            job.setMapperClass(CountMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(CountReduce.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.setCombinerClass(CountCombiner.class);

            job.setNumReduceTasks(4);

            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }

            boolean b = job.waitForCompletion(true);
            ReadOutput.read(outPath);
            return b ? 0 :1;
        }
}
