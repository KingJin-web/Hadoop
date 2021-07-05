package com.king.D0703.B;

import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

    public static class TaskMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private boolean dataSource;
        private IntWritable ONE = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException {
            FileSplit fs = (FileSplit) context.getInputSplit();
            String fileName = fs.getPath().getName();
            if (fileName.startsWith("POS")) {
                dataSource = true;
            } else if (fileName.startsWith("NET")) {
                dataSource = false;
            } else {
                throw new IOException("File Name should starts with POS or NET");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] array = value.toString().split("\t");
            String loc, time, imsi;
            Text word = new Text();
            imsi = array[0];
            if (dataSource) {
                loc = array[3];
                time = array[4].split(":")[0];
            } else {
                loc = array[2];
                time = array[3].split(":")[0];
            }
            word.set(time + "|" + loc);
            context.write( word,new Text(imsi));
        }
    }

    public static class TaskReduce1 extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> setKey = new HashSet<>();
            for (Text val : values){
                setKey.add(String.valueOf(val));
            }
            context.write(key,new IntWritable(setKey.size()));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);
        Job job = Job.getInstance(super.getConf(), "统计每天每时间段在每个基站的人数");
        job.setJarByClass(JZ1.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(TaskMapper1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TaskReduce1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            args = new String[4];
            args[0] = "D:\\wordcount\\input\\D0701";
            args[1] = "D:\\wordcount\\output\\D0701";
            args[2] = "2021-07-01";
            args[3] = "07-09-17-24";
        }
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
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

        ReadOutput.readAll(args[1]);
        System.exit(i);
    }
}
