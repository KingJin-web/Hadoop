package com.king.HM.H.p1_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-26 09:21
 */
public class CountReduce extends Reducer<Text, LongWritable, Text, Text> {

    //车辆总和
    private double all = 0;
    private Map<String, Long> map = new HashMap<>();


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("countReduce - reduce");
        long sum= 0;
        for (LongWritable val : values){
            sum+= val.get();

        }
        all += sum;
        map.put(key.toString(),sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("countReduce - cleanup");
        Set<String> keySet = map.keySet();
        for (String key : keySet){
            long v = map.get(key);
            double percent = v / all;
            context.write(new Text(key),new Text(v + "\t" + percent));
        }
    }
}
