package com.king.HM.G;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 20:53
 */
public class WeatherMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {
    public WeatherMapper() {
        System.out.println("Mapper");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split("\t");
        //System.out.println(Arrays.toString(s));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        try {
            Date d = dateFormat.parse(s[0]);
            calendar.setTime(d);

            Weather w = new Weather();
            w.setYear(calendar.get(Calendar.YEAR));
            w.setMonth(calendar.get(Calendar.MONTH) + 1);
            w.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            int degree = Integer.parseInt(s[1].substring(0, s[1].lastIndexOf("c")));
            w.setDegree(degree);
           System.out.println(w);
            context.write(w, new IntWritable(degree));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}

