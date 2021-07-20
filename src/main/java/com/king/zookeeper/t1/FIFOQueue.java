package com.king.zookeeper.t1;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @auther omen
 * @create 2021-07-17-21:26
 */
public class FIFOQueue<T> {
    static AtomicInteger integer = new AtomicInteger(1);
    //客户端
    protected final ZkClient zkClient;

    protected final String root;

    protected static final String Node_name = "n_";

    public FIFOQueue(ZkClient zkClient, String root) {
        this.zkClient = zkClient;
        this.root = root;
    }

    public int size() {
        return zkClient.getChildren(root).size();
    }

    public boolean isEmpty() {
        return zkClient.getChildren(root).size() == 0;
    }

    //将节点offer到队列中，但是要保证顺序
    public boolean offer(T element) throws Exception {
        System.out.println(Thread.currentThread() + "向里面添加了" + element.toString() + "元素");
        String path = root.concat("/").concat(Node_name);
        try {
            zkClient.createEphemeralSequential(path, element);
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(root);
            offer(element);
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
        return true;
    }

    //拿到编号最小的，把最小数据删除
    public T poll() throws Exception {
        try {
            List<String> childrens = zkClient.getChildren(root);
            if (childrens.size() == 0) {
                return null;
            }
            Collections.sort(childrens, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return getNodeNumber(o1, Node_name).compareTo(getNodeNumber(o2, Node_name));
                }
            });
            String litterNode = childrens.get(0);
            String fullPath = root.concat("/").concat(litterNode);
            T data = (T) zkClient.readData(fullPath);
            zkClient.delete(fullPath);
            return data;
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
    }

    private String getNodeNumber(String str, String nodeName) {
        int index = str.lastIndexOf(nodeName);
        if (index >= 0) {
            index += Node_name.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("node1:2181,node2:2181,node3:2181");
        FIFOQueue<Integer> queue = new FIFOQueue<>(zkClient,"/root");
        new Thread(new ConsumerThread(queue)).start();
        new Thread(new ConsumerThread(queue)).start();
        new Thread(new ProducerThread(queue)).start();
        new Thread(new ProducerThread(queue)).start();
        new Thread(new ProducerThread(queue)).start();
    }

    static class ProducerThread implements Runnable {
        FIFOQueue<Integer> queue;

        ProducerThread() {

        }

        ProducerThread(FIFOQueue<Integer> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            for (int i = 0; i < 100; i++) {
                try {
                    Thread.sleep((int) (Math.random() * 500));
                    queue.offer(FIFOQueue.integer.getAndIncrement());
                } catch (Exception e) {
                }
            }
        }
    }

    static class ConsumerThread implements Runnable {
        FIFOQueue<Integer> queue;

        ConsumerThread() {
        }

        ConsumerThread(FIFOQueue<Integer> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            for (int i = 0; i < 100; i++) {
                try {
                    Thread.sleep((int) (Math.random() * 1000));
                    System.out.println(Thread.currentThread() + "取出了:" + queue.poll());
                } catch (Exception e) {
                }
            }
        }
    }
}
