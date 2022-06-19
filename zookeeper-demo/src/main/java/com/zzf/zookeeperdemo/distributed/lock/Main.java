package com.zzf.zookeeperdemo.distributed.lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Main {

    public static void main(String[] args) {
        Thread thread1 = new Thread(new UserThread(), "user1");
        Thread thread2 = new Thread(new UserThread(), "user2");

        thread1.start();
        thread2.start();
    }

//    static Lock lock = new ReentrantLock();
    static ZKlock1 lock = new ZKlock1();

    static class UserThread implements Runnable {

        @Override
        public void run() {
            new Order().createOrder();
            lock.lock();
            boolean result = new Stock().reduceStock();
            lock.unlock();
            if (result) {
                System.out.println(Thread.currentThread().getName()+"减库存成功");
                new Pay().pay();
            } else {
                System.out.println(Thread.currentThread().getName()+"减库存失败");
            }
        }
    }
}
