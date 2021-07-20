package com.king.zookeeper.master2;

/**
 * @auther omen
 * @create 2021-07-07-20:21
 */


import com.king.zookeeper.ZKHelper;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * 主要功能: 1. 监听/command的变化情况，接收操作命令
 * 2. 监听/servers的子节点变化情况，更新节点列表
 */
public class ManageServer {
    //zookeeper的servers节点路径
    private String serversPath;
    //zookeeper的command节点路径
    private String commandPath;
    //zookeeper的config节点路径
    private String configPath;
    private ZooKeeper zkClient;
    private ServerConfig config;
    //用于监听servers节点的子节点列表的变化
    private Watcher childListener;
    //用于监听command节点数据内容的变化
    private Watcher dataListener;
    //工作服务器的列表
    private List<String> workServerList;

    public static void main(String[] args) throws IOException, InterruptedException {
        ZKHelper zh = new ZKHelper();
        ZooKeeper zk = zh.connect();
        //微服务的配置信息
        ServerConfig sc = new ServerConfig();
        sc.setDbUser("root");
        sc.setDbPwd("a");
        sc.setDbUrl("jdbc:localhost:1433/test");
        ManageServer ms = new ManageServer("/servers", "/command", "/config", zk, sc);
        ms.start();

        System.out.println("敲回车键退出!\n");
        new BufferedReader(new InputStreamReader(System.in)).readLine();//阻塞

        ms.stop();
    }

    public ManageServer(String serversPath, String commandPath, String configPath, ZooKeeper zkClient, ServerConfig config) {
        this.serversPath = serversPath;
        this.commandPath = commandPath;
        this.zkClient = zkClient;
        this.config = config;
        this.configPath = configPath;
        //监听servers节点的子节点列表的变化
        this.childListener = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //子节点列表发生变更
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        workServerList = zkClient.getChildren(serversPath, childListener);
                        System.out.println("work server list changed,new list is");
                        execList();
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        //监听command节点数据内容的变化
        this.dataListener = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //  /command节点发生变化
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    Stat stat = new Stat();
                    try {
                        byte[] bs = zkClient.getData(commandPath, dataListener, stat);
                        String cmd = new String(bs);
                        System.out.println("cmd:" + cmd);
                        exeCmd(cmd);  //执行命令
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    private void initRunning() {
        Stat stat1 = new Stat();
        try {
            zkClient.getData(commandPath, dataListener, stat1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        Stat stat2 = new Stat();
        try {
            zkClient.getChildren(serversPath, childListener, stat2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void exeCmd(String cmdType) {
        if ("list".equals(cmdType)) {
            execList();
        } else if ("create".equals(cmdType)) {
            execCreate();
        } else if ("modify".equals(cmdType)) {
            execModify();
        } else {
            System.out.println("error command!" + cmdType);
        }
    }

    //列除工作服务器列表
    private void execList() {
        if (workServerList != null && workServerList.size() > 0) {
            System.out.println(workServerList.toString());
        } else {
            System.out.println("暂无服务器注册");
        }
    }

    //创建config节点
    private void execCreate() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            config.serialize(boa, "header");
            System.out.println("创建的配置信息为:" + config.toString());
            if (zkClient.exists(configPath, false) == null) {
                try {
                    String r = zkClient.create(configPath, baos.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println("创建的配置信息为:" + config.toString());
                } catch (KeeperException e) {
                    System.out.println("更新*****配置信息为:" + config.toString());
                    //config节点已经存在，则写入内容就可以了
                    zkClient.setData(configPath, baos.toByteArray(), zkClient.exists(configPath, true).getVersion());
                    e.printStackTrace();
                }
            } else {
                System.out.println("更新*****配置信息为:" + config.toString());
                //config节点已经存在，则写入内容就可以了
                zkClient.setData(configPath, baos.toByteArray(), zkClient.exists(configPath, true).getVersion());
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //修改config节点内容
    private void execModify() {
        try {
            //我们随意修改config的一个属性就可以了
            config.setDbUser(config.getDbUser() + "_modify");//写死了，将来是由controller server发过来的值决定
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            config.serialize(boa, "header");
            zkClient.setData(configPath, baos.toByteArray(), zkClient.exists(configPath, true).getVersion());
        } catch (Exception e) {
            execCreate(); // 写入时config节点还未存在，则创建它
        }
    }

    //启动工作服务器
    public void start() {
        initRunning();
    }

    //停止工作服务器
    public void stop() {
        try {
            zkClient.getChildren(serversPath, false);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        Stat stat = new Stat();
        try {
            zkClient.getData(commandPath, false, stat);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
