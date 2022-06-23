package com.zzf.zookeeperdemo.distributed.config;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        Config config = new Config();
        config.save("timeout", "100");

        for (int i = 0; i < 100; i++) {
            System.out.println(config.get("timeout"));

            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
