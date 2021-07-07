package com.king.zookeeper;

import org.apache.commons.lang.text.CompositeFormat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-05 20:38
 */
public class ZKHelper {
    public static String connectString = "node1:2181,node2:2181,node3:2181";
    public static int sessionTimeout = 2000;
    private static ZooKeeper zkClient = null;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ZooKeeper connect() {
        System.out.println("zk 初始化中。。。。。");
        try {
            zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("事件信息" + event.getType() + "--" + event.getPath() + "--" + event.getState());
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        System.out.println("zk客户端建立连接。。。。 ");
                        countDownLatch.countDown();
                    }

                }
            });

            countDownLatch.await();
            System.out.println("客户端主进程运行完");

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return zkClient;
    }

    public void close() {
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String ls(String path) {
        List<ACL> list;
        try {
            list = zkClient.getACL(path, null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return " ";
    }


    public static String getConnectString() {
        return connectString;
    }

    public static int getSessionTimeout() {
        return sessionTimeout;
    }

    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String printZnodeInfo(Stat stat) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n*******************************\n");
        sb.append("创建znode的事务id czxid:" + stat.getCzxid() + "\n");
        sb.append("创建znode的时间 ctime:" + df.format(stat.getCtime()) + "\n");
        sb.append("更新znode的事务id mzxid:" + stat.getMzxid() + "\n");
        sb.append("更新znode的时间 mtime:" + df.format(stat.getMtime()) + "\n");
        sb.append("更新或删除本节点或子节点的事务id pzxid:" + stat.getPzxid() + "\n");
        sb.append("子节点数据更新次数 cversion:" + stat.getCversion() + "\n");
        sb.append("本节点数据更新次数 dataVersion:" + stat.getVersion() + "\n");
        sb.append("节点ACL(授权信息)的更新次数 aclVersion:" + stat.getAversion() + "\n");
        if (stat.getEphemeralOwner() == 0) {
            sb.append("本节点为持久节点\n");
        } else {
            sb.append("本节点为临时节点,创建客户端id为:" + stat.getEphemeralOwner() + "\n");
        }
        sb.append("数据长度为:" + stat.getDataLength() + "字节\n");
        sb.append("子节点个数:" + stat.getNumChildren() + "\n");
        sb.append("\n*******************************\n");
        return sb.toString();
    }
}
