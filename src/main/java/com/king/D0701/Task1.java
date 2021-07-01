package com.king.D0701;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;

/**
 * @program: hdfs
 * @description: 先计算出不同用户在不同时段在不同基站停留的时间.
 * @author: King
 * @create: 2021-07-01 19:53
 */
public class Task1 extends Configured implements Tool {
    enum Counter{
        TIMESKIP, //时间格式有误
        OUTOFTIMESKIP, // 时间不在指定时间段类
        LINESKIP, //源文件有误
        USERSKIP //

    }
    public static class TaskMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private static final IntWritable outputValue = new IntWritable(1);
        private TableLine tl = new TableLine();
        private String date;
        private String[] timepoint;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.date = context.getConfiguration().get("date");
            this.timepoint = context.getConfiguration().get("timepoint").split("-");
            FileSplit fs = (FileSplit) context.getInputSplit();
            String fileName = fs.getPath().getName();
            if (fileName.startsWith("")){
                
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            try {
                tl.set(s, !s.endsWith("www"), "2021-07-01", new String[]{"07", "17", "24"});
            } catch (LineException e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("date",strings[3]);
        conf.set("timepoint",strings[4]);
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(strings[0]);
        Path outPath = new Path(strings[1]);

        return 0;
    }
}
