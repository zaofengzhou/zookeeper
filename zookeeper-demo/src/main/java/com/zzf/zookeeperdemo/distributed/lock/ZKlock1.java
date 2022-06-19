package com.zzf.zookeeperdemo.distributed.lock;

import com.zzf.zookeeperdemo.service.CuratorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ZKlock1 implements Lock {

    private CuratorFramework curatorClient;

    private ThreadLocal<CuratorFramework> zk = new ThreadLocal<>();

    private String LOCK_NAME = "/LOCK";
    private ThreadLocal<String> CURRENT_NODE = new ThreadLocal<>();

    public void init() {
        if (zk.get() == null) {
            zk.set(new CuratorService().getCuratorClient());
        }
    }

    public void lock() {
        init();
        //
        if (tryLock()) {
            System.out.println(Thread.currentThread().getName()+"已经获取到锁了....");
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {  // 阻塞
        String nodeName = LOCK_NAME + "/zk_";

        try {
            // /LOCK/zk_1
            CURRENT_NODE.set(zk.get().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(nodeName, new byte[0]));

            List<String> list = zk.get().getChildren().forPath(LOCK_NAME);   // zk_1, zk_2
            Collections.sort(list);

            String minNodeName = list.get(0);

            if (CURRENT_NODE.get().equals(LOCK_NAME+"/"+minNodeName)) {
                return true;
            } else {
                // currentIndex,
                // currentIndex-1

                String currentNodeSimpleName = CURRENT_NODE.get().substring(CURRENT_NODE.get().lastIndexOf("/")+1);
                Integer currentNodeIndex = list.indexOf(currentNodeSimpleName);
                String prevNodeName = list.get(currentNodeIndex-1);

                // 阻塞
                final CountDownLatch countDownLatch = new CountDownLatch(1);

                Watcher w = new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if (Event.EventType.NodeDeleted.equals(watchedEvent.getType())) {
                            // ...
                            countDownLatch.countDown();
                            System.out.println(Thread.currentThread().getName()+"被唤醒...");
                        }
                    }
                };
                zk.get().getData().usingWatcher(w).forPath(LOCK_NAME+"/"+prevNodeName);

//                zk.get().exists(LOCK_NAME + "/" + prevNodeName, new Watcher() {
//                    @Override
//                    public void process(WatchedEvent watchedEvent) {
//                        if (Event.EventType.NodeDeleted.equals(watchedEvent.getType())) {
//                            // ...
//                            countDownLatch.countDown();
//                            System.out.println(Thread.currentThread().getName()+"被唤醒...");
//                        }
//                    }
//                });
                System.out.println(Thread.currentThread().getName()+"阻塞...");
                countDownLatch.await();
                return true;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public void unlock() {
        try {
            zk.get().delete().forPath(CURRENT_NODE.get());
            CURRENT_NODE = null;
            zk.get().close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public Condition newCondition() {
        return null;
    }

}
