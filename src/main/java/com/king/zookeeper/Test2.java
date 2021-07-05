package com.king.zookeeper;

import org.apache.zookeeper.ZooKeeper;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-05 21:22
 */
public class Test2 {
    private static ZooKeeper zk;
    private static ZKHelper zkHelper;

    public static void main(String[] args) {
        zkHelper = new ZKHelper();
        zk = zkHelper.connect();
        System.out.println("zk信息" + zk);
        zkHelper.close();
        System.out.println("客户端运行完毕 关闭连接");
    }
}
