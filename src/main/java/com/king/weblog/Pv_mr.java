package com.king.weblog;

import com.king.util.ReadOutput;
import com.king.weblog.A.A_App;
import com.king.weblog.A.A_Mapper;
import com.king.weblog.A.A_Reduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-30 20:39
 */
public class Pv_mr extends Configured implements Tool {


    public static class KpiPvMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private static final IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //KPI kpi = KPI.parser(value.toString());
            KPI kpi = KPI.fileterPVs(value.toString());
            if (kpi.isValid()){
                outputKey.set(kpi.getRequest());
                context.write(outputKey, outputValue);
            }

        }

    }

    public static class KpiPvReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //统计单词数
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            //将输出的结果放到context 里面
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new Pv_mr(), args);

        System.exit(i);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path("D:\\wordcount\\input\\data\\access.20120104.log");
        Path outPath = new Path("D:\\wordcount\\output\\data");
        Job job = Job.getInstance(super.getConf(), "IP");
        job.setJarByClass(Pv_mr.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(KpiPvMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(KpiPvReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean b = job.waitForCompletion(true);
        new ReadOutput(outPath);
        return b ? 0 : 1;
    }
}
