package com.king.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-06 19:48
 */
public class Test7 {
    private static ZooKeeper zk;
    private static ZKHelper zkHelper;

    public static void delete(String path) throws InterruptedException, KeeperException {
        zk.delete(path, zk.exists(path, true).getVersion());
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {
        zkHelper = new ZKHelper();
        zk = zkHelper.connect();
        System.out.println("zk信息" + zk);
        delete("/servers");
        zkHelper.close();
        System.out.println("客户端运行完毕 关闭连接");
    }
}
