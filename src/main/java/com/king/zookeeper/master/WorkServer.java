package com.king.zookeeper.master;

import com.king.zookeeper.ZKHelper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import lombok.SneakyThrows;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-06 20:29
 */
public class WorkServer {

    //记录服务器状态
    private volatile boolean running = false;

    private ZooKeeper zkClient;
    //Master节点对应zookeeper中的节点路径
    private static final String MASTER_PATH = "/master";
    //监听Master节点删除事件
    private Watcher watch;
    //记录当前节点的基本信息
    private RunningData serverData;
    //记录集群中Master节点的基本信息
    private RunningData masterData;
    //线程池
    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private int delayTime = 5;

    public WorkServer(RunningData rd) {
        this.serverData = rd;   //记录服务器基本信息
        this.watch = new Watcher() {
            @Override
            public void process(WatchedEvent we) {
                try {
                    //临听节点被删除
                    if (we.getType() == Event.EventType.NodeDeleted) {
                        if (masterData != null && masterData.getName().equals(serverData.getName())) {
                            //自己就是上一轮的Master服务器,直接抢
                            takeMaster();
                        } else {
                            //TODO:优化策略.否则,延迟5秒后再抢.主要是应对网络抖动.给上一轮的Master服务器优先抢占master的权利,避免不必要的数据迁移开销
                            //如果不加这个延迟代码的话,则各个客户端都可以抢到/master.
//                            delayExector.schedule(new Runnable() {
//                                @Override
//                                public void run() {
                            takeMaster();
//                                }
//                            }, delayTime, TimeUnit.SECONDS);
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };
    }

    public ZooKeeper getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZooKeeper zkClient) {
        this.zkClient = zkClient;
    }

    //启动服务器
    public void start() throws Exception {
        if (running) {
            throw new Exception("服务已经启动...");
        }
        running = true;
        //订阅Master节点删除事件
        Stat stat = new Stat();

        //1.在/servers下注册自己
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        serverData.serialize(boa, "header");
        //在/servers/下创建自己服务器的节点信息,相当于服务注册,临时节点.
        String r = zkClient.create("/servers/" + serverData.getCid(), baos.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        try {
            zkClient.getData(MASTER_PATH, watch, stat);
        } catch (KeeperException.NoNodeException ex) {
            //没有这个节点
            //争取Master权利
            takeMaster();
        }
    }

    //停止服务器
    public void stop() throws Exception {
        if (!running) {
            throw new Exception("服务器已经停止...");
        }
        running = false;
        delayExector.shutdown();
        //释放Master权利
        releaseMaster();
    }

    //争抢Master
    private void takeMaster() {
        if (!running) {
            return;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            serverData.serialize(boa, "header");

            //尝试创建Master临时节点
            String r = zkClient.create(MASTER_PATH, baos.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            masterData = serverData;
            System.out.println(serverData.getName() + "成为了服务器master");

            //TODO:作为演示,我们让服务器每隔5秒释放一次Master权利,上线的服务器代码中没有,它就是关闭
            //方案一
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    Thread.sleep(5);
//                    if (checkMaster()){
//                        releaseMaster();
//                    }
//                }
//            }).start();
            //方案二:线程池
            delayExector.schedule(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    if (checkMaster()) {
                        releaseMaster();
                    }
                }
            }, 5, TimeUnit.SECONDS);
        } catch (KeeperException.NoNodeException e) {
            //节点/master已经创建了,则获取节点信息.
            try {
                //已被其它服务器创建了
                //读取Master节点信息,重要是绑定监听器
                Stat stat = new Stat();
                byte[] result = zkClient.getData(MASTER_PATH, watch, stat);
                if (result == null) {
                    takeMaster();   //没读到,读取瞬间Master节点宕机了,有机会再次争抢
                } else {
                    //这里这是一个测试代码,用于获取当前正执有master节点的服务器的信息,以便后用.
                    ByteBuffer bb = ByteBuffer.wrap(result);
                    ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
                    BinaryInputArchive bia = BinaryInputArchive.getArchive(bbis);
                    RunningData header = new RunningData();
                    header.deserialize(bia, "create");
                    masterData = header;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    //释放Master权利
    private void releaseMaster() throws KeeperException, InterruptedException {
        if (checkMaster()) {
            zkClient.delete(MASTER_PATH, zkClient.exists(MASTER_PATH, true).getVersion());
        }
    }

    //检测自己是否为Master
    private boolean checkMaster() {
        try {
            Stat stat = new Stat();
            byte[] result = zkClient.getData(MASTER_PATH, watch, stat);
            ByteBuffer bb = ByteBuffer.wrap(result);
            ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
            BinaryInputArchive bia = BinaryInputArchive.getArchive(bbis);
            RunningData header = new RunningData();
            header.deserialize(bia, "create");
            masterData = header;
            if (masterData.getName().equals(serverData.getName())) {
                return true;
            }
            return false;
        } catch (Exception ex) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        RunningData rd = new RunningData();
        rd.setCid(1L);
        rd.setName("hello");
        ZKHelper zh =  new ZKHelper();

        WorkServer ws = new WorkServer(rd);
        ws.setZkClient(zh.connect());
        ws.start();
        Thread.sleep(10);

        //ws.stop();
    }
}
