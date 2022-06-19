package com.zzf.zookeeperdemo.service;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CuratorService {
    // Zookeeper 服务器地址
    @Value("${curator.connectString}")
    private String connectString;
    // session 会话超时时间
    @Value("${curator.sessionTimeoutMs}")
    private int sessionTimeoutMs;
    // 名称空间：在操作节点时，会以 namespace 为父节点
    @Value("${curator.namespace}")
    private String namespace;

    /**
     * session 重连策略，使用其中一种即可
     */
    // RetryForever：间隔{参数1}毫秒后重连，永远重试
    private RetryPolicy retryForever = new RetryForever(3000);

    // RetryOneTime：{参数1}毫秒后重连，只重连一次
    private RetryPolicy retryOneTime = new RetryOneTime(3000);

    // RetryNTimes： {参数2}毫秒后重连，重连{参数1}次
    private RetryPolicy retryNTimes = new RetryNTimes(3,3000);

    // RetryUntilElapsed：每{参数2}毫秒重连一次，总等待时间超过{参数1}毫秒后停止重连
    private RetryPolicy retryUntilElapsed = new RetryUntilElapsed(10000,3000);

    // ExponentialBackoffRetry：可重连{参数2}次，并增加每次重连之间的睡眠时间，增加公式如下：
    // {参数1} * Math.max(1,random.nextInt(1 << ({参数2：maxRetries} + 1)))
    private RetryPolicy exponential = new ExponentialBackoffRetry(1000,3);

    /**
     * 获取 CuratorClient
     * 使用 Fluent 风格
     * @return CuratorFramework
     */
    public CuratorFramework getCuratorClient(){
        // 使用 CuratorFrameworkFactory 来构建 CuratorFramework
        return CuratorFrameworkFactory.builder()
                // Zookeeper 服务器地址字符串
                .connectString(connectString)
                // session 会话超时时间
                .sessionTimeoutMs(sessionTimeoutMs)
                // 使用哪种重连策略
                .retryPolicy(retryOneTime)
                // 配置父节点
                .namespace(namespace)
                .build();
    }
}
