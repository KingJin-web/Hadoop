package com.king.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-06 19:39
 */
public class Test6_2 {
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
        if (stat == null) {
            System.out.println("stat 不存在");
            return;
        }
       showTree(path,0);
        zkHelper.close();
        System.out.println("客户端运行完毕 关闭连接");
    }

    public static void showTree(String path, int level) {
        Stat stat = null;
        try {
            stat = znode_exists(path);
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < level; i++) {
                sb.append("\t");
            }
            System.out.println(sb + path);
            List<String> children = null;
            try {
                children = zk.getChildren(path, false);
            } catch (Exception e) {
                return;
            }
            for (String sunPath : children) {

                if (level == 0) {
                    showTree(path + sunPath, level + 1);
                } else {
                    showTree(path + "/" + sunPath, level + 1);
                }
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
