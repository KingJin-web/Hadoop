package com.king.wc.B;

import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(super.getConf(),"排序");
        job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\wordcount\\input\\B"));

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(CountBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(CountBean.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outPath = new Path("D:\\wordcount\\output\\B");
        //
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        TextOutputFormat.setOutputPath(job,outPath);

        boolean b = job.waitForCompletion(true);

        return b ? 0 :1;
    }

    public static void main(String[] args) throws Exception {
        int i = ToolRunner.run(new JobMain(), args);
        ReadOutput.read("D:\\wordcount\\output\\B");
        System.exit(i);
    }
}
