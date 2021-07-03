package com.king.D0703.B;

import com.king.util.ReadOutput;
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
 * @description: 统计每天每时间段在每个基站的人数.
 * @author: King
 * @create: 2021-07-01 19:53
 */
public class JZ1 extends Configured implements Tool {
    enum Counter {
        TIMESKIP, //时间格式有误
        OUTOFTIMESKIP, // 时间不在指定时间段类
        LINESKIP, //源文件有误
        USERSKIP //

    }

    public static class TaskMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private static final IntWritable outputValue = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        // 用户IMSI|基站|时间段|时长
        private String imsi;
        private String position;
        private String timeFlag;
        private String time;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] s1 =value.toString().split("\\|");
            imsi = s1[0];
            position = s1[1];
            timeFlag = s1[2];

            outputKey.set(position + "|" + timeFlag);
            context.write(outputKey, outputValue);

        }
    }

    public static class TaskReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();
        // 用户IMSI|基站|时间段|时长

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                for(IntWritable v : values){
                    count += v.get();
                }
                outputKey.set(key);
                outputValue.set(count);
                context.write(outputKey, outputValue);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("date", strings[2]);
        conf.set("timepoint", strings[3]);
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(strings[0]);
        Path outPath = new Path(strings[1]);
        Job job = Job.getInstance(super.getConf(), "基站");
        job.setJarByClass(JZ1.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(TaskMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TaskReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean b = job.waitForCompletion(true);

        ReadOutput.readAll(outPath);
        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        if (args == null || args.length < 1) {
            args = new String[4];
            args[0] = "D:\\wordcount\\input\\D0703";
            args[1] = "D:\\wordcount\\output\\D0703";
            args[2] = "2021-07-01";
            args[3] = "07-09-17-24";
        }
        if (args.length != 4) {
            System.err.println("");
            System.err.println("Usage: BaseStationDataPreprocess < input path > < output path > < date > < timepoint >");
            System.err.println("Example: BaseStationDataPreprocess /user/james/Base /user/james/Output 2012-09-12 07-09-17-24");
            System.err.println("Warning: Timepoints should be begined with a 0+ two digit number and the last timepoint should be 24");
            System.err.println("Counter:");
            System.err.println("\t" + "TIMESKIP" + "\t" + "Lines which contain wrong date format");
            System.err.println("\t" + "OUTOFTIMESKIP" + "\t" + "Lines which contain times that out of range");
            System.err.println("\t" + "LINESKIP" + "\t" + "Lines which are invalid");
            System.err.println("\t" + "USERSKIP" + "\t" + "Users in some time are invalid");
            System.exit(-1);
        }

        int i = ToolRunner.run(new JZ1(), args);

        System.exit(i);
    }
}
