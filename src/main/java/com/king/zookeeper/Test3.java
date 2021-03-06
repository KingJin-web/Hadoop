package com.king.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-05 21:27
 */
public class Test3 {
    private static ZooKeeper zk;
    private static ZKHelper zkHelper;

    public static void create(String path, byte[] bytes) throws InterruptedException, KeeperException {

        String s = zk.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建节点 " + s);
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {

        System.out.println(Arrays.toString("你好".getBytes(StandardCharsets.UTF_8)));
        zkHelper = new ZKHelper();
        zk = zkHelper.connect();
        System.out.println("zk信息" + zk);
        create("/aa", "aa".getBytes());
        zkHelper.close();
        System.out.println("客户端运行完毕 关闭连接");
    }
}
