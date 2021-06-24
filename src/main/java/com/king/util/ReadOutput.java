package com.king.util;

import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.Path;

/**
 * @program: hdfs
 * @description: 读计算后的part-r-00000文件
 * @author: King
 * @create: 2021-06-23 00:29
 */
public class ReadOutput {
    private String readPath;

    public ReadOutput() {
        System.out.println("计算后结果: ");
    }

    public ReadOutput(Path readPath) {
        System.out.println("计算后结果: ");
        this.readPath = readPath + "\\part-r-00000";
        read();
    }

    public ReadOutput(String readPath) {
        System.out.println("计算后结果: ");
        this.readPath = readPath + "\\part-r-00000";
        read();
    }

    public void read() {

        File file = new File(readPath);
        try (BufferedReader bin = new BufferedReader(new FileReader(file))) {
            String s;
            while ((s = bin.readLine()) != null) {
                System.out.println(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void read(Path path) {
        System.out.println("计算后结果: ");
        String readPath = path + "\\part-r-00000";
        File file = new File(readPath);
        try (BufferedReader bin = new BufferedReader(new FileReader(file))) {
            String s;
            while ((s = bin.readLine()).getBytes(StandardCharsets.UTF_8) != null) {
                System.out.println(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void read(String path) {
        System.out.println("计算后结果: ");
        String readPath = path + "\\part-r-00000";
        File file = new File(readPath);
        try (BufferedReader bin = new BufferedReader(new FileReader(file))) {
            String s;
            while ((s = bin.readLine()) != null) {
                System.out.println(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
