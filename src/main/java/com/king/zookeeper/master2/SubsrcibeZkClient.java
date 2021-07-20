package com.king.zookeeper.master2;

import com.king.zookeeper.ZKHelper;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-16 15:23
 */
public class SubsrcibeZkClient {
    private static final int CLIENT_QTY = 5;
    private static final String CONFIG_PATH = "/config";
    private static final String COMMAND_PATH = "/command";
    private static final String SERVERS_PATH = "/servers";

    public static void main(String[] args) {
        List<ZooKeeper> clients = new ArrayList<>();
        List<WorkServer> workServers = new ArrayList<>();
        ManageServer manageServer = null;
        try {
            ServerConfig initConfig = new ServerConfig();
            initConfig.setDbPwd("123346");
            initConfig.setDbUrl("jdbc:mysql://你的数据库地址:3306/kingcloud");
            initConfig.setDbUser("root");
            ZKHelper zkHelper = new ZKHelper();

            ZooKeeper clientMange = zkHelper.connect();
            manageServer = new ManageServer(SERVERS_PATH, COMMAND_PATH, CONFIG_PATH, clientMange, initConfig);
            manageServer.start();
            for (int i = 0; i < CLIENT_QTY; i++) {
                ZooKeeper client = zkHelper.connect();
                clients.add(client);
                ServerData serverData = new ServerData();
                serverData.setId(i);
                serverData.setName("WorkServer#" + i);
                serverData.setAddress("192.168.1." + i);

                WorkServer workServer = new WorkServer(CONFIG_PATH, SERVERS_PATH, serverData, client, initConfig);
                workServers.add(workServer);
                workServer.start();
            }
            System.out.println("按回车退出 ！");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println();
            try {
                for (WorkServer workServer : workServers) {
                    workServer.stop();
                }
                for (ZooKeeper client : clients) {
                    client.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
