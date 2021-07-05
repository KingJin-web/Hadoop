package com.king.D0703.B;

import com.king.D0701.LineException;
import com.king.D0701.TableLine;
import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @program: hdfs
 * @description: c. 统计涉嫌养号的手机.
 * 一个手机号对应了多个sim卡号.
 * @author: King
 * @create: 2021-07-03 19:36
 */
public class JZ3 extends Configured implements Tool {
    /**
     * 计数器
     * 用于计数各种异常数据
     */
    enum Counter {
        TIMESKIP,        //时间格式有误
        OUTOFTIMESKIP,   //时间不在参数指定的时间段内
        LINESKIP,        //源文件行有误
        USERSKIP         //某个用户某个时间段被整个放弃
    }

    /**
     * 读取一行数据
     */
    public static class JZ3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        String date;
        String[] timepoint;
        boolean dataSource;

        private Text t1 = new Text();
        private Text t2 = new Text();

        /**
         * 初始化:   setup()执行一次，读取配置
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.date = context.getConfiguration().get("date");     //读取日期
            this.timepoint = context.getConfiguration().get("timepoint").split("-");  //读取时间分割点
            //提取输入的文件名，判断是POS.txt 还是NET.txt
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

        /**
         * MAP任务
         * 读取基站数据
         * 找出数据所对应时间段
         * 以IMSI和时间段作为KEY
         * CGI和实践作为VALUE
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            TableLine tableLine = new TableLine();
            //读取行
            try {
                tableLine.set(line, this.dataSource, this.date, this.timepoint);
            } catch (LineException e) {
                if (e.getFlag() == -1) {
                    //mapreuce 中的一个  Map<xxx ,数量
                    context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
                } else {
                    context.getCounter(Counter.TIMESKIP).increment(1);
                }
                return;
            } catch (Exception e) {
                context.getCounter(Counter.LINESKIP).increment(1);
                return;
            }
            t1.set(tableLine.getImsi());  //sim卡
            t2.set(tableLine.getImei());  //手机编号
            context.write(t2, t1);
        }
    }

    public static class JZ3Reduce extends Reducer<Text, Text, Text, NullWritable> {

        private HashSet<String> im = new HashSet<>();


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (Text i : values) {
                if (im.add(i.toString())) {
                    sum++;
                }
            }
            if (sum > 2) {
                context.write(key, NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //读取命令行参数，存到Configuration中,这样Mapper和reducer就可以访问到了
        conf.set("date", args[2]);
        conf.set("timepoint", args[3]);

        Job job = Job.getInstance(conf, "统计涉嫌养号的手机");
        job.setJarByClass(JZ3.class);

        //输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //输出路径
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        //创建 输出 文件，并判断 这个输出 文件是否存在
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        //调用上面Map类作为Map任务代码
        job.setMapperClass(JZ3Mapper.class);
        //调用上面Reduce类最为Reduce任务代码
        job.setReducerClass(JZ3Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            args = new String[4];
            args[0] = "D:\\wordcount\\input\\D0701";
            args[1] = "D:\\wordcount\\output\\D0701";
            args[2] = "2021-07-01";
            args[3] = "07-09-17-24";
        }

        int res = ToolRunner.run(new Configuration(), new JZ3(), args);

        ReadOutput.readAll(args[1]);
        System.exit(res);
    }
}
