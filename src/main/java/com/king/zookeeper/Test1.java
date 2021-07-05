package com.king.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * @program: hdfs
 * @description: zookeeper 实例一
 * @author: King
 * @create: 2021-07-05 20:37
 */
public class Test1 {
    private static ZooKeeper zkClient = null;
    private static Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            System.out.println("事件信息" + event.getType() + "--" + event.getPath() + "--" + event.getState());
            try {
                List<String> list = zkClient.getChildren("/",true);
                if (list != null){
                    for (String s : list){
                        System.out.println(s);
                    }
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

        }
    };

    public static void main(String[] args) throws IOException {
        zkClient = new ZooKeeper(ZKHelper.connectString, ZKHelper.sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("事件信息" + event.getType() + "--" + event.getPath() + "--" + event.getState());
                try {
                    List<String> list = zkClient.getChildren("/",true);
                    if (list != null){
                        for (String s : list){
                            System.out.println(s);
                        }
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
        System.out.println("zk信息" + zkClient);
    }
}
