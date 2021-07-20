package com.example.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @auther omen
 * @create 2021-07-18-16:12
 */

/**
 * @author wong
 * @date 2021/7/4 9:15
 */
@RestController
public class Controller {

    @Autowired
    private StringRedisTemplate redisTemplate;
    //    private ReentrantLock lock=new ReentrantLock();//单机锁
    InterProcessMutex lock = new InterProcessMutex(getZkClient(), "/locks");//框架锁

    @RequestMapping(value = "/secKill",method = RequestMethod.GET)
    public String secKill() throws Exception {
        //因为自实现的锁是不可重入锁，所以每个请求到来时都需要单独创建对象建立连接，性能低
//        DistributedLock lock=new DistributedLock("node1:2181");//自实现锁
        try{
            //lock.lock();
            lock.acquire();
            int num=Integer.parseInt(redisTemplate.opsForValue().get("goods"));
            if(num>0){
                int realNum=num-1;
                redisTemplate.opsForValue().set("goods",realNum+"");
                System.out.println("抢购第"+num+"张票");
                return "抢购成功";
            }else{
                System.out.println("库存不足");
                return "库存不足";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "系统异常";
        } finally {
            lock.release();
            //lock.unlock();
        }
    }

    private static CuratorFramework getZkClient(){
        //重试策略
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("node1:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy)
                .build();
        client.start();
        return client;
    }
}
