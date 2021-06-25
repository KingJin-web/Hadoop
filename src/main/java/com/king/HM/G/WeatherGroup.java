package com.king.HM.G;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-25 19:49
 */
public class WeatherGroup extends WritableComparator {
    public WeatherGroup() {
        super(Weather.class, true);
        System.out.println("WeatherGroup");
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        System.out.println("Group 的比较");
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        int c1 = Integer.compare(w1.getYear(), w2.getYear());
        if (c1 == 0) {
            return Integer.compare(w1.getMonth(), w2.getMonth());
        }
        return c1;
    }
}
