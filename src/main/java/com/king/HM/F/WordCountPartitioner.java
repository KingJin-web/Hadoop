package com.king.HM.F;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 21:25
 */
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        System.out.println("注意分区数: " + i);
        int a = text.hashCode()%i;
        System.out.println("分区a值：" + a);
        if (a >= 0){
            return a;
        }
        return 0;
    }
}
