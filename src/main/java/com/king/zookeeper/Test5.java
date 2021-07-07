package com.king.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.nio.charset.StandardCharsets;

/**
 * @program: hdfs 更新节点数据
 * @description:
 * @author: King
 * @create: 2021-07-06 19:28
 */
public class Test5 {
    private static ZooKeeper zk;
    private static ZKHelper zkHelper;

    public static void update(String path, byte[] data) throws InterruptedException, KeeperException {
        zk.setData(path, data, zk.exists(path, true).getVersion());
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {
        zkHelper = new ZKHelper();
        zk = zkHelper.connect();
        System.out.println("zk信息" + zk);
        update("/aa", "钟浣文小宝贝".getBytes(StandardCharsets.UTF_8));
        zkHelper.close();
        System.out.println("客户端运行完毕 关闭连接");
    }
}
