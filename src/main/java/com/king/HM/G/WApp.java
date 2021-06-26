package com.king.HM.G;

import com.king.HM.D.WcReducer;
import com.king.HM.F.UidApp;
import com.king.HM.F.UidMapper;
import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.FileInputStream;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-25 19:36
 */
public class WApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new WApp(), args);

        System.exit(i);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path("D:\\wordcount\\input\\G");
        Path outPath = new Path("D:\\wordcount\\output\\G");
        Job job = Job.getInstance(super.getConf(),"天气");
        job.setJarByClass(WApp.class);

        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.setMapperClass(WeatherMapper.class);


        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Weather.class);
        job.setOutputValueClass(IntWritable.class);

        job.setSortComparatorClass(WeatherSort.class);
        job.setGroupingComparatorClass(WeatherGroup.class);
        job.setPartitionerClass(WeatherPartition.class);

        job.setNumReduceTasks(1);

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean b = job.waitForCompletion(true);
        ReadOutput.read(outPath);
        return b ? 0 :1;
    }
}
