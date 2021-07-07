package com.king.zookeeper.master;

import com.king.zookeeper.ZKHelper;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-06 20:51
 */
public class LeaderSelectorZkClient {
    private static final int CLIENT_QTY = 10;

    public static void main(String[] args) {
        //保存所有zkClients的表
        List<ZooKeeper> clients = new ArrayList<>();
        List<WorkServer> workServers = new ArrayList<>();

        ZKHelper zkHelper = new ZKHelper();
        try {
            for (int i = 0; i < CLIENT_QTY; ++i) {
                ZooKeeper client = zkHelper.connect();
                clients.add(client);

                RunningData runningData = new RunningData();
                runningData.setCid((long) i);
                runningData.setName("Client #" + i);
                //创建服务
                WorkServer workServer = new WorkServer(runningData);
                workServer.setZkClient(client);
                workServers.add(workServer);

                workServer.start();

            }
            System.out.println("按回车退出  ");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("服务关闭。。。");
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