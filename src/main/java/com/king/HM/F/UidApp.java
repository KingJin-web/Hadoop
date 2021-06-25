package com.king.HM.F;

import com.king.HM.B.JobMain;
import com.king.HM.D.NewKey;
import com.king.HM.D.WcMapper;
import com.king.HM.D.WcReducer;
import com.king.HM.E.App;
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

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 21:18
 */
public class UidApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new UidApp(), args);
        ReadOutput.read("D:\\wordcount\\output\\F");
        System.exit(i);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path("D:\\wordcount\\input\\F\\data.txt");
        Path outPath = new Path("D:\\wordcount\\output\\F");
        Job job = Job.getInstance(super.getConf(),"二次排序");
        job.setJarByClass(UidApp.class);
        job.setCombinerClass((Class<? extends Reducer>) Reducer.class);
        job.setNumReduceTasks(1);



        job.setMapperClass(UidMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(UidReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
       // job.setPartitionerClass(WordCountPartitioner.class);


        FileInputFormat.addInputPath(job,inPath);



        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        boolean b = job.waitForCompletion(true);

        return b ? 0 :1;
    }
}
