package com.king;

import com.king.util.IPSeeker;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class IPSeekerTest {

    private IPSeeker ip = IPSeeker.getInstance();

    @Test
    public void getInstance() throws InterruptedException {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yy:HH:mm:ss");
        Date date = new Date();

        long a = new Date().getTime();
        byte[] bytes = new byte[]{47, 98, 33, 64};
        String s1 = ip.getAddress(bytes);
        long b = new Date().getTime();
        System.out.println(s1 + " " + (b - a));

        long c = new Date().getTime();
        String s2 = ip.getAddress("218.77.74.132");
        long d = new Date().getTime();
        System.out.println(s2 + " " + (d - c));
    }

    public static void main(String[] args) {
        String 名字 = "蔡徐坤";
        int 年龄 = 19;
        System.out.println(名字 + 年龄);
    }
}

class Test1
{
    private int data;
    int result = 0;
    public void m()
    {
        result += 2;
        data += 2;
        System.out.print(result + "  " + data);
        System.out.println("");
    }
}
class ThreadExample extends Thread
{
    private Test1 mv;
    public ThreadExample(Test1 mv)
    {
        this.mv = mv;
    }
    public void run()
    {
        synchronized(mv)
        {
            mv.m();
        }
    }
}
class ThreadTest
{
    public static void main(String args[])
    {
        Test1 mv = new Test1();
        Thread t1 = new ThreadExample(mv);
        Thread t2 = new ThreadExample(mv);
        Thread t3 = new ThreadExample(mv);
        t1.start();
        t2.start();
        t3.start();
    }
}