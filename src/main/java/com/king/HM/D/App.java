package com.king.HM.D;

import com.king.HM.B.CountBean;
import com.king.HM.B.JobMain;
import com.king.HM.B.SortMapper;
import com.king.HM.B.SortReduce;
import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 19:14
 */
public class App extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(super.getConf(),"二次排序");
        job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\wordcount\\input\\D"));

        job.setMapperClass(WcMapper.class);
        job.setMapOutputKeyClass(NewKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(WcReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outPath = new Path("D:\\wordcount\\output\\D");

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        TextOutputFormat.setOutputPath(job,outPath);

        boolean b = job.waitForCompletion(true);

        return b ? 0 :1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new App(), args);
        ReadOutput.read("D:\\wordcount\\output\\D");
        System.exit(i);
    }
}
