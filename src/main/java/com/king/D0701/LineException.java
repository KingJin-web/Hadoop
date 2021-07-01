package com.king.D0701;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-01 20:39
 */
public class LineException extends Exception {
    int flag;
    public LineException(String msg, int flag) {
        super(msg);
        this.flag = flag;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
