package com.king.wc.B;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,CountBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key: " + key + " value: " + value);
        CountBean bean = new CountBean();
        bean.setNum(Integer.parseInt(value.toString()));
        context.write(bean,NullWritable.get());
    }

}
