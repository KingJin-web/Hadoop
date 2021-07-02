//package com.king.test;
//
//import com.king.util.IPSeeker;
//import org.junit.Test;
//
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//
//import static org.junit.Assert.*;
//
//public class IPSeekerTest {
//
//    private IPSeeker ip = new IPSeeker();
//
//    @Test
//    public void getInstance() throws InterruptedException {
//        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yy:HH:mm:ss");
//        Date date = new Date();
//
//        long a = new Date().getTime();
//        byte[] bytes = new byte[]{47, 98, 33, 64};
//        String s1 = ip.getAddress(bytes);
//        long b = new Date().getTime();
//        System.out.println(s1 + " " + (b - a));
//
//        long c= new Date().getTime();
//        String s2 = ip.getAddress("218.77.74.132");
//        long d = new Date().getTime();
//        System.out.println(s2 + " " + (d - c));
//    }
//
//}