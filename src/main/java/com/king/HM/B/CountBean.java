package com.king.HM.B;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountBean implements WritableComparable<CountBean> {
    private int num;

    @Override
    public int compareTo(CountBean o) {
        return this.num - o.num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.num = in.readInt();
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return num + "\t";
    }
}
