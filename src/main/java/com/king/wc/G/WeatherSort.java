package com.king.wc.G;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 21:01
 */
public class WeatherSort extends WritableComparator {
    public WeatherSort() {
        super(Weather.class, true);
        System.out.println("WeatherSort");
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        System.out.println("调用Sort 中的 compare");
        System.out.println(a);
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        System.out.println(a);
        int c1 = Integer.compare(w1.getYear(), w2.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(w1.getMonth(), w2.getMonth());
            if (c2 == 0) {
                return -Integer.compare(w1.getDegree(), w2.getDegree());
            }
            return c2;
        }
        return c1;
    }
}
