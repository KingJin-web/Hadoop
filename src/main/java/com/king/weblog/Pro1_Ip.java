package com.king.weblog;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-30 21:10
 */
public class Pro1_Ip extends Configured implements Tool {


    public static class KpiPvMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.parser(value.toString());

            if (kpi.isValid()) {
                outputKey.set(kpi.getRequest());
                outputValue.set(kpi.getRemote_addr());
                System.out.println(kpi.getRemote_addr());
                context.write(outputKey, outputValue);
            }


        }

    }

    public static class KpiPvReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();
        private Set<String> count = new HashSet<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //统计单词数

            for (Text value : values) {
                System.out.println(value.toString());
                count.add(value.toString());
            }
            outputValue.set(String.valueOf(count.size()));
            //将输出的结果放到context 里面
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        //BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new Pro1_Ip(), args);

        System.exit(i);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path("D:\\wordcount\\input\\data\\data.txt");
        Path outPath = new Path("D:\\wordcount\\output\\data");
        Job job = Job.getInstance(super.getConf(), "IP");
        job.setJarByClass(Pro1_Ip.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(KpiPvMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(KpiPvReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean b = job.waitForCompletion(true);
        new ReadOutput(outPath);
        return b ? 0 : 1;

    }
}
