package com.zzf.zookeeperdemo.service;

import org.I0Itec.zkclient.ZkClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ZkClientServer {
    // Zookeeper 集群的地址
    @Value(value = "${zookeeper.address}")
    private String address;
    // 获取 ZkClient
    public ZkClient getZkClient() {
        return new ZkClient(address);
    }
}
