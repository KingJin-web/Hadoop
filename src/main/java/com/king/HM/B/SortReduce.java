package com.king.HM.B;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<CountBean, NullWritable, LongWritable, CountBean> {

    private int num = 0;
    private int count = 0;
    public SortReduce() {
        System.out.println("Reduce 构造方法");
    }

    @Override
    protected void reduce(CountBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        for (NullWritable value : values) {
            // System.out.println(value);
            num++;
        }
        context.write(new LongWritable(num), key);
    }
}
