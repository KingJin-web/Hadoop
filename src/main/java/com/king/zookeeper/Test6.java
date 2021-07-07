package com.king.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Stack;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-06 19:33
 */
public class Test6 {
    private static ZooKeeper zk;
    private static ZKHelper zkHelper;

    public static Stat znode_exists(String path) throws InterruptedException, KeeperException {
        return zk.exists(path, true);
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {
        String path = "/";
        zkHelper = new ZKHelper();
        zk = zkHelper.connect();
        System.out.println("zk信息" + zk);
        Stat stat = znode_exists(path);
        if (stat == null){
            System.out.println("stat 不存在");
            return;
        }
        List<String> children = zk.getChildren(path,false);
        System.out.println("path: " + path + " 子节点如下：");
        for (String s: children){
            System.out.println(s);
        }
        zkHelper.close();
        System.out.println("客户端运行完毕 关闭连接");
    }
}
