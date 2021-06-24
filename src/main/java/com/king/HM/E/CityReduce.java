package com.king.HM.E;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 19:43
 */
public class CityReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text v : values){
            if (v.toString().startsWith(SomeCityMapper.LABEL)){
                i = 1;
                break;
            }
        }
        if (i == 0){
            context.write(key,new Text(""));
        }
    }
}
