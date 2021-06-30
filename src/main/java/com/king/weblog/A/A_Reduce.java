package com.king.weblog.A;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-27 18:41
 */
public class A_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outputValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //统计单词数
        int count = 0;
        for (IntWritable value : values) {
            count = count + value.get();
        }
        //将输出的结果放到context 里面
        context.write(key, new IntWritable(count));
    }
}
