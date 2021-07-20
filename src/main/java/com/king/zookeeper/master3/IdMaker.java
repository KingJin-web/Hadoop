package com.king.zookeeper.master3;

import com.king.zookeeper.ZKHelper;
import lombok.SneakyThrows;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @program: hdfs
 * @description: 利用zookeeper生成唯一id
 * https://www.cnblogs.com/xiufengchen/p/10339072.html
 * @author: King
 * @create: 2021-07-16 16:13
 */
public class IdMaker {
    private ZKHelper zk = new ZKHelper();
    private ZooKeeper client = null;
    //记录服务器的地址
    private String server;
    //记录父节点的路径
    private String root;
    //节点的名称
    private String nodeName;

    private volatile boolean running = false;
    private ExecutorService cleanExector = null;


    private void checkRunning() {
    }

    //删除节点的级别
    public enum RemoveMethod {
        NONE, IMMEDIATELY, DELAY

    }

    public IdMaker(String root, String nodeName) {
        this.root = root;
        this.nodeName = nodeName;
    }

    public void start() throws Exception {

        if (running)
            throw new Exception("server has stated...");
        running = true;

        init();

    }

    private void init() {


        client = zk.connect();
        cleanExector = Executors.newFixedThreadPool(10);
        try {
            cleanExector = Executors.newFixedThreadPool(10);
            if (client.exists(root, null) == null) {
                String r = client.create(root, "".getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void stop() throws Exception {

        if (!running)
            throw new Exception("server has stopped...");
        running = false;

        freeResource();

    }

    private void freeResource() {
        cleanExector.shutdown();
        try {
            cleanExector.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            cleanExector = null;
        }
        if (client != null) {

            try {
                client.close();
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            client = null;
        }
    }

    public String generateId(RemoveMethod removeMethod) throws InterruptedException, KeeperException {
        checkRunning();
        final String fullNodePath = root.concat("/").concat(nodeName);
        final String ourPath = client.create(fullNodePath, "".getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(ourPath);

        /**
         * 在创建完节点后为了不占用太多空间，可以选择性删除模式
         */
        if (removeMethod.equals(RemoveMethod.IMMEDIATELY)) {
            client.delete(ourPath, client.exists(ourPath, null).getVersion());
        } else if (removeMethod.equals(RemoveMethod.DELAY)) {
            cleanExector.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    client.delete(ourPath, client.exists(ourPath, null).getVersion());
                }
            });

        }
        //node-0000000000, node-0000000001，ExtractId提取ID
        return ExtractId(ourPath);
    }

    private String ExtractId(String str) {
        int index = str.lastIndexOf(nodeName);
        if (index >= 0) {
            index += nodeName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;

    }

}
