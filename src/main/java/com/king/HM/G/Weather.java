package com.king.HM.G;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**t
 *
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 20:56
 */
public class Weather implements WritableComparable<Weather> {
    private int year;
    private int month;
    private int day;
    private int degree;

    @Override
    public String toString() {
        return "Weather{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", degree=" + degree +
                '}';
    }

    @Override
    public int compareTo(Weather o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
