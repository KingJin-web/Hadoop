package com.king.MapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-20 15:57
 */
public class WordCount {
// TokenizerMapper作为Map阶段，需要继承Mapper，并重写map()函数
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

    // 用StringTokenizer作为分词器，对value进行分词
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        // 输入键
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            // 输入键  输出值 输入键  输出值
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {

                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();     //new配置对象，默认读取顺序是default-site.xml<core-site.xml
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        // 给Combiner 设置了IntSumReducer  针对一个mapper 的结果
        job.setReducerClass(IntSumReducer.class);
        // 给Combiner 设置了IntSumReducer  针对两个mapper 的结果
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path inPath = new Path("D:\\wordcount\\input\\Hello");
        Path outPath = new Path("D:\\wordcount\\output\\Hello");
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        int i = job.waitForCompletion(true) ? 0 : 1;
        new ReadOutput(outPath);

        System.exit(i);

    }
}