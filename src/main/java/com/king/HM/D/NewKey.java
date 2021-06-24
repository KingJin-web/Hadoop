package com.king.HM.D;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 18:59
 */
public class NewKey implements WritableComparable<NewKey> {
    private long first;
    private long second;

    @Override
    public int compareTo(NewKey o) {
        long v1 = this.first - o.first;
        if (v1 == 0) {
            return (int) (this.second - o.second);
        }
        return (int) v1;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(first);
        dataOutput.writeLong(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readLong();
        this.second = dataInput.readLong();
    }

    public NewKey() {
    }

    public NewKey(long first, long second) {
        this.first = first;
        this.second = second;
    }
}
