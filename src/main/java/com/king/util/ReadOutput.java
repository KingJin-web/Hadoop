package com.king.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

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
        this.readPath = readPath + "";
        read();
    }

    public ReadOutput(String readPath) {
        this.readPath = readPath;
        read();
    }

    public void read() {
        read(readPath);
    }

    /**
     * 读取所有生成的 part-r-00000 文件
     *
     * @param path
     */
    public static void read(String path) {
        System.out.println("计算后结果: ");
        String readPath = path + "\\part-r-00000";
        File file = new File(readPath);
        try (BufferedReader bin = new BufferedReader(new FileReader(file))) {
            String s;
            int count = 0;
            while ((s = bin.readLine()) != null) {
                count++;
                System.out.println(s);
            }
            System.out.println(count + " 条数据");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void read(Path path) {
        System.out.println(path.toUri());
        System.out.println(path);
        read(path + "");
    }

    /**
     * 读取所有生成的 part-r-* 文件
     *
     * @param path
     */
    public static void readAll(String path) { readAll(path, "part-r-", false); }

    /**
     * 读取所有生成的 part-r-* 文件
     *
     * @param path
     */
    public static void readAll(String path, boolean bool) {
        readAll(path, "part-r-", bool);
    }

    /**
     * 读取文件夹下 指定前缀名的文件
     *
     * @param path      文件路径
     * @param startName 文件前缀名
     * @param bool      是否遍历全部
     */
    public static void readAll(String path, String startName, boolean bool) {
        File file = new File(path);
        File[] files = file.listFiles();



        int count = 0;
        assert files != null;
        for (File value : files) {
            String fileName = value.getName();

            if (value.isFile() && fileName.startsWith(startName)) {
                try (InputStreamReader reader = new InputStreamReader(new FileInputStream(value));
                     BufferedReader br = new BufferedReader(reader);) {
                    String line = "";
                    System.out.println("读取文件: " + fileName);
                    while ((line = br.readLine()) != null) {
                        System.out.println(line);
                        count++;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (bool && !fileName.endsWith(".crc") && !fileName.endsWith("_SUCCESS")) {
                System.out.println("文件夹： " + value.getName());
                readAll(value.getPath(), startName, true);
            }

        }
        System.out.println(count + "条数据");
    }

    public static void readAll(Path path) {
        readAll(path.toString());
    }

    void test() {
        File file = new File(readPath);
        try (BufferedReader bin = new BufferedReader(new FileReader(file))) {
            String s;
            int count = 0;
            while ((s = bin.readLine()) != null && count <= 1000) {
                System.out.println(s);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String readPath = "D:\\wordcount\\output\\D0701";
        Path outPath = new Path("D:\\wordcount\\output\\H");
        ReadOutput.readAll(readPath,true);

    }
}
