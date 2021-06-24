package com.king.HM.A;

import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        Job job = Job.getInstance(super.getConf(), "quchong1");
        job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("D:\\wordcount\\input\\A"));

        job.setMapperClass(RemovalMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(RemovalReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outPath = new Path("D:\\wordcount\\output\\A");
        //如果存在就删了它
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        TextOutputFormat.setOutputPath(job, outPath);
        //Thread.sleep(10000000);
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境

        int i = ToolRunner.run(new JobMain(), args);
        Path outPath = new Path("D:\\wordcount\\output\\A");

        new ReadOutput(outPath);

        System.exit(i);
    }
}
