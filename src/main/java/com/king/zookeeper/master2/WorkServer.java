package com.king.zookeeper.master2;


import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ByteBufferInputStream;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.*;
import java.nio.ByteBuffer;


/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-16 14:41
 */
public class WorkServer {
    private ZooKeeper zkClient;
    private String configPath;
    private String serverPath;
    private ServerData serverData;
    private ServerConfig serverConfig;

    private Watcher watcher;

    public WorkServer(String configPath, String serverPath, ServerData serverData, ZooKeeper zkClient, ServerConfig initConfig) {
        this.zkClient = zkClient;
        this.configPath = configPath;
        this.serverPath = serverPath;
        this.serverData = serverData;
        this.serverConfig = initConfig;

        this.watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        Stat stat = new Stat();
                        byte[] result = zkClient.getData(configPath, watcher, stat);
                        ByteBuffer bb = ByteBuffer.wrap(result);
                        ByteBufferInputStream bbis = new ByteBufferInputStream(bb);;
                        BinaryInputArchive bia = BinaryInputArchive.getArchive(bbis);
                        ServerConfig header = new ServerConfig();
                        header.deserialize(bia,"create");


                    }
                } catch (KeeperException | InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        };

    }

    public void start(){
        System.out.println("work server start ....");
        initRunning();
    }
    public void stop(){
        System.out.println("work server stop ....");

    }
    private void initRunning() {
        registMe();

        Stat stat = new Stat();

        try {
            zkClient.getData(configPath,this.watcher,stat);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void registMe() {
        String mePath = serverPath.concat("/").concat(serverData.getAddress());
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            serverConfig.serialize(boa,"header");
            String r = zkClient.create(mePath,baos.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);


            System.out.println(r);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
