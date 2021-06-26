package com.king.HM.G;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * t
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
        int t1 = Integer.compare(this.year,o.getYear());
        if(t1 == 0){
            int t2 = Integer.compare(this.month,o.getMonth());
            if (t2 == 0){
                return Integer.compare(this.degree , o.getDegree());

            }
            return t2;
        }
        return t1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.write(month);
        dataOutput.write(day);
        dataOutput.write(degree);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.year = dataInput.readInt();
        System.out.println(year);
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.degree = dataInput.readInt();

    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }
}
