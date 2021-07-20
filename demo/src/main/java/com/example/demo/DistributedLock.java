package com.example.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @auther omen
 * @create 2021-07-18-16:37
 */
public class DistributedLock {
    private final int sessionTimeOut=2000;
    private final ZooKeeper zk;
    //根节点
    private final String LOCK="/locks";
    //同步工具类，构造传入计数值，当计数达到0，释放所有等待的线程
    private CountDownLatch connectLatch=new CountDownLatch(1);
    private CountDownLatch waitLatch=new CountDownLatch(1);
    //监听上一个节点的路径
    private String waitPath;
    //当前节点
    private String currentNode;

    public DistributedLock(String connectString) throws IOException, InterruptedException, KeeperException {
        //获取连接
        zk=new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //连接上zk，释放
                if(event.getState()==Event.KeeperState.SyncConnected){
                    //计数-1
                    connectLatch.countDown();
                }
                if(event.getType()==Event.EventType.NodeDeleted && event.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }
            }
        });
        //等待zk正常连接后，程序往下执行
        //在计数器为0之前一直等待
        connectLatch.await();
        Stat stat = zk.exists(LOCK, false);
        //判断根节点 /locks 是否存在，不存在就创建根节点
        if(stat==null){
            //创建下一个根节点
            zk.create(LOCK,"locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
    }

    //加锁
    public void lock() {
        try {
            //创建对应的临时带序号节点
            currentNode= zk.create(LOCK + "/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            //判断当前要创建的节点是否是序号最小的节点，如果是获取到锁，不是则监听前一个节点
            List<String> children = zk.getChildren(LOCK, false);
            //如果children只有一个值，说明就是自己，直接获取锁；如果有多个，需要判断谁最小
            if(children.size()==1){
                return;
            }else{
                Collections.sort(children);
                //获取节点名称 seq-00000000
                String thisNode = currentNode.substring((LOCK+"/").length());
                //通过 seq-00000000获取该节点在children集合的位置
                int index = children.indexOf(thisNode);
                if(index==0){
                    //说明只有一个节点，获取锁
                    return;
                }else{
                    //需要监听前一个节点的变化
                    waitPath=LOCK+"/"+children.get(index-1);
                    zk.getData(waitPath,true,new Stat());
                    //等待监听
                    waitLatch.await();
                    return;
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //解锁
    public void unlock(){
        try {
            //删除节点
            zk.delete(currentNode,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}

