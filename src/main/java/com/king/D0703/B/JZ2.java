package com.king.D0703.B;

import com.king.D0701.LineException;
import com.king.D0701.TableLine;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-03 16:40
 */
public class JZ2 extends Configured implements Tool {
    //TODO  统计基站的总数， 本运营商注册的手机号个数。
    enum Counter {
        LINESKIP, //源文件有误
        OUTOFTIMESKIP, // 时间不在指定时间段类
        TIMESKIP, //时间格式有误
        USERSKIP //


    }

    public static class JZ2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private TableLine tl = new TableLine();
        private String date;
        private String[] timepoint;
        private boolean dataSource;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.date = context.getConfiguration().get("date");
            this.timepoint = context.getConfiguration().get("timepoint").split("-");
            FileSplit fs = (FileSplit) context.getInputSplit();
            String fileName = fs.getPath().getName();
            if (fileName.startsWith("POS")) {
                dataSource = true;
            } else if (fileName.startsWith("NET")) {
                dataSource = false;
            } else {
                throw new IOException("File name should start with POS or NET");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            try {
                tl.set(s, this.dataSource, this.date, this.timepoint);
            } catch (LineException e) {
                if (e.getFlag() == -1) {
                    context.getCounter(JZ2.Counter.OUTOFTIMESKIP).increment(1);
                } else {
                    context.getCounter(JZ2.Counter.TIMESKIP).increment(1);
                }
                return;
            } catch (Exception e) {
                context.getCounter(JZ2.Counter.LINESKIP).increment(1);
                return;
            }
            System.out.println(tl);
            outputKey.set(tl.getPosition());
            outputValue.set(tl.getImsi());
            context.write(outputKey, outputValue);

        }
    }

    public static class JZ2Reduce extends Reducer<Text, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private static IntWritable outputValue = new IntWritable(1);
        private HashSet<String> sims = new HashSet<>();
        ;
        int count1 = 0; //sim 卡数量
        int count2 = 0; //基站 卡数量

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            System.out.println(key.toString());
            for (Text v : values) {
                if (sims.add(v.toString())) {
                    sum++;
                }
            }
            count1 += sum;
            count2++;
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputKey.set("基站总数");
            outputValue.set(count2);
            context.write(outputKey, outputValue);
            outputKey.set("sim卡数量");
            outputValue.set(count1);
            context.write(outputKey, outputValue);
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
        Job job = Job.getInstance(super.getConf(), "统计基站的总数， 本运营商注册的手机号个数。");
        job.setJarByClass(JZ2.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(JZ2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JZ2Reduce.class);
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
            args[0] = "D:\\wordcount\\input\\D0701";
            args[1] = "D:\\wordcount\\output\\D0701";
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

        int i = ToolRunner.run(new JZ2(), args);

        System.exit(i);
    }
}
