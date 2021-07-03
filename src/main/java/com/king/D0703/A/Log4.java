package com.king.D0703.A;

import com.king.util.ReadOutput;
import com.king.weblog.KPI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * @program: hdfs
 * @description: d. 统计每天访问的人数.
 * @author: King
 * @create: 2021-07-03 16:03
 */
public class Log4 extends Configured implements Tool {
    public static class Log4PvMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private static final IntWritable outputValue = new IntWritable(1);
        SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
        SimpleDateFormat sdf2 = new SimpleDateFormat("YYYY-MM-dd");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String time = KPI.GetDate(value.toString());
            try {
                if (time.equals("Other")) {
                    context.write(outputKey, outputValue);
                    return;
                }
                String day = sdf2.format(sdf1.parse(time));
                outputKey.set(day);
                context.write(outputKey, outputValue);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            //System.out.println(browser);


        }


    }

    public static class Log4PvReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text outputKey = new Text();
        private static IntWritable outputValue = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //统计单词数
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            outputValue.set(count);
            context.write(key, outputValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path("D:\\wordcount\\input\\data\\access.log.10");
        Path outPath = new Path("D:\\wordcount\\output\\data");
        Job job = Job.getInstance(super.getConf(), "统计浏览器数量");
        job.setJarByClass(Log4.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(Log4PvMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Log4PvReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean b = job.waitForCompletion(true);
        new ReadOutput(outPath);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new Log4(), args);

        System.exit(i);
    }
}
