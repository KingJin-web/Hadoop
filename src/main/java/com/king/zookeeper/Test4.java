package com.king.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-06 18:43
 */
public class Test4 {
    private static ZooKeeper zk;
    private static ZKHelper zkHelper;
    private static CountDownLatch countDownLatch = new CountDownLatch(10); //绑定监听十次

    public static Stat znode_exists(String path) throws InterruptedException, KeeperException {
        return zk.exists(path, true);
    }

    public static void main(String[] args) {


        try {
            String path = "/aa";
            zkHelper = new ZKHelper();
            zk = zkHelper.connect();
            Stat stat = znode_exists(path);
            if (stat == null) {
                System.out.println("stat 不存在");
                return;
            }
            byte b[] = zk.getData(path, new MyWatch(path, countDownLatch, zk), stat);
            String data = new String(b, StandardCharsets.UTF_8);
            System.out.println("从主程序中获取数据节点： " + path + "的原始数据为：" + data);

            String info = ZKHelper.printZnodeInfo(stat);
            System.out.println(info);
            countDownLatch.await();
            System.out.println("主程序运行结束");
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        zkHelper.close();
    }
}

class MyWatch implements Watcher {
    String path;
    CountDownLatch countDownLatch;
    ZooKeeper zk;

    public MyWatch(String path, CountDownLatch countDownLatch, ZooKeeper zk) {
        this.path = path;
        this.countDownLatch = countDownLatch;
        this.zk = zk;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
            Stat stat = new Stat();
            try {
                byte[] bytes = zk.getData(path, MyWatch.this, stat);
                String data = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("从主程序中获取数据节点： " + path + "的新 数据为：" + data);

                String info = ZKHelper.printZnodeInfo(stat);
                System.out.println(info);
                countDownLatch.countDown();

            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

        } else {
            System.out.println("事件类型为" + event.getType());
        }
    }
}
