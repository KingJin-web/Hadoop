package com.king.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
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

                    List<String> list = null;


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

    public void close(){
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getConnectString() {
        return connectString;
    }

    public static int getSessionTimeout() {
        return sessionTimeout;
    }
}
