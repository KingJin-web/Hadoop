package com.king.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

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

        String s = zk.create(path,bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建节点 " + s);
    }
}
