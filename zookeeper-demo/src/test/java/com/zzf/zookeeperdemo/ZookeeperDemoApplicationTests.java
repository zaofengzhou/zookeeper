package com.zzf.zookeeperdemo;

import com.zzf.zookeeperdemo.service.CuratorService;
import com.zzf.zookeeperdemo.service.ZkClientServer;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
class ZookeeperDemoApplicationTests {

    // 依赖注入
    @Autowired
    private ZkClientServer zkClientServer;

    @Autowired
    private CuratorService curatorService;

    @Test
    void contextLoads() {
        // 获取 ZkClient 对象
        ZkClient zkClient = zkClientServer.getZkClient();
        // 获取子节点集合
        List<String> children = zkClient.getChildren("/");
        System.out.println(children);
        // 释放资源
        zkClient.close();
    }

    @Test
    public void testZookeeper() throws Exception {
        ZooKeeper client = new ZooKeeper("106.15.50.100:2181", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // ..
                System.out.println("连接的时候"+watchedEvent);
            }
        });
        int allChildrenNumber = client.getAllChildrenNumber("/");
        System.out.println(allChildrenNumber);
    }

    @Test
    public void testCreate() {
        ZkClient zkClient = zkClientServer.getZkClient();
        // 在 imooc 节点下创建持久节点 wiki
        zkClient.createPersistent("/imooc/wiki");
        // 获取 imooc 节点的子节点集合
        List<String> children = zkClient.getChildren("/imooc");
        System.out.println(children);
        // 释放资源
        zkClient.close();
    }

    @Test
    public void testDelete() {
        ZkClient zkClient = zkClientServer.getZkClient();
        // 删除 imooc 的 子节点 wiki
        boolean delete = zkClient.delete("/imooc/wiki");
        System.out.println(delete);
        // 释放资源
        zkClient.close();
    }

    @Test
    public void testWriteAndRead() {
        ZkClient zkClient = zkClientServer.getZkClient();
        // 给 imooc 节点写入数据 wiki
        zkClient.writeData("/imooc","wiki");
        // 读取 imooc 节点的数据
        Object data = zkClient.readData("/imooc");
        System.out.println(data);
        // 释放资源
        zkClient.close();
    }

    @Test
    public void testCuratorCreate() throws Exception {
        // 获取客户端
        CuratorFramework curatorClient = curatorService.getCuratorClient();
        // 开启会话
        curatorClient.start();
        // 在 namespace 下创建节点 Mooc , 节点前需要加 “/” 表示命名空间下的子节点
        // 节点内容为 Wiki ,使用字节数组传入
        String mooc = curatorClient.create().forPath("/Mooc", "Wiki".getBytes());
        // 返回 /Mooc
        System.out.println(mooc);
        curatorClient.close();
    }

    @Test
    public void testLs() throws Exception {
        // 获取客户端
        CuratorFramework curatorClient = curatorService.getCuratorClient();
        // 开启会话
        curatorClient.start();
        // 查询命名空间下的子节点
        List<String> strings = curatorClient.getChildren().forPath("/");
        System.out.println(strings);
        curatorClient.close();
    }

    @Test
    public void testGetData() throws Exception {
        // 获取客户端
        CuratorFramework curatorClient = curatorService.getCuratorClient();
        // 开启会话
        curatorClient.start();
        // 获取 Mooc 节点的内容
        byte[] bytes = curatorClient.getData().forPath("/Mooc");
        // 输出
        System.out.println(new String(bytes));
        curatorClient.close();
    }

    @Test
    public void testSetData() throws Exception {
        // 获取客户端
        CuratorFramework curatorClient = curatorService.getCuratorClient();
        // 开启会话
        curatorClient.start();
        // 更新节点数据，返回当前节点状态
        Stat stat = curatorClient.setData().forPath("/Mooc", "wiki".getBytes());
        // 输出
        System.out.println(stat);
        curatorClient.close();
    }

    @Test
    public void testDeleteData() throws Exception {
        // 获取客户端
        CuratorFramework curatorClient = curatorService.getCuratorClient();
        // 开启会话
        curatorClient.start();
        // 删除节点
        curatorClient.delete().forPath("/Mooc");
        curatorClient.close();
    }

    @Test
    public void testCurator() throws Exception {
        // 获取客户端
        CuratorFramework curatorClient = curatorService.getCuratorClient();
        // 开启会话
        curatorClient.start();
        // CuratorWatcher 为接口，我们需要实现 process 方法
        CuratorWatcher watcher = new CuratorWatcher(){
            @Override
            // 监听事件处理
            public void process(WatchedEvent watchedEvent) {
                // 输出 监听事件
                System.out.println(watchedEvent.toString());
            }
        };
        // 在命名空间下创建持久节点 mooc，内容为 Wiki
        curatorClient.create().forPath("/mooc","Wiki".getBytes());
        // 获取 mooc 节点的 data 数据，并对 mooc 节点开启监听
        byte[] bytes = curatorClient.getData().usingWatcher(watcher).forPath("/mooc");
        // 输出 data
        System.out.println(new String(bytes));
        // 第一次更新
        curatorClient.setData().forPath("/mooc", "Wiki001".getBytes());

        curatorClient.getData().usingWatcher(watcher).forPath("/mooc");
        // 第二次更新
        curatorClient.setData().forPath("/mooc","Wiki002".getBytes());

        curatorClient.getData().usingWatcher(watcher).forPath("/mooc");
        curatorClient.delete().forPath("/mooc");
    }

    @Test
    public void testCuratorCacheListener() throws Exception {
        // 获取客户端
        CuratorFramework client = curatorService.getCuratorClient();
        // 开启会话
        client.start();
        // 构建 CuratorCache 实例
        CuratorCache cache = CuratorCache.build(client, "/mooc");
        // 使用 Fluent 风格和 lambda 表达式来构建 CuratorCacheListener 的事件监听
        CuratorCacheListener listener = CuratorCacheListener.builder()
                // 开启对所有事件的监听
                // type 事件类型：NODE_CREATED, NODE_CHANGED, NODE_DELETED;
                // oldNode 原节点：ChildData 类，包括节点路径，节点状态 Stat，节点 data
                // newNode 新节点：同上
                .forAll((type, oldNode, newNode) -> {
                    System.out.println("forAll 事件类型：" + type);
                    System.out.println("forAll 原节点：" + oldNode);
                    System.out.println("forAll 新节点：" + newNode);
                })
                // 开启对节点创建事件的监听
                .forCreates(childData -> {
                    System.out.println("forCreates 新节点：" + childData);
                })
                // 开启对节点更新事件的监听
                .forChanges((oldNode, newNode) -> {
                    System.out.println("forChanges 原节点：" + oldNode);
                    System.out.println("forChanges 新节点：" + newNode);
                })
                // 开启对节点删除事件的监听
                .forDeletes(oldNode -> {
                    System.out.println("forDeletes 原节点：" + oldNode);
                })
                // 初始化
                .forInitialized(() -> {
                    System.out.println("forInitialized 初始化");
                })
                // 构建
                .build();

        // 注册 CuratorCacheListener 到 CuratorCache
        cache.listenable().addListener(listener);
        // CuratorCache 开启缓存
        cache.start();
        // mooc 节点创建
        client.create().forPath("/mooc");
        // mooc 节点更新
        client.setData().forPath("/mooc","Wiki".getBytes());
        // mooc 节点删除
        client.delete().forPath("/mooc");
    }

    @Test
    public void testCuratorEphemeral() throws Exception {
        // 获取客户端
        CuratorFramework client = curatorService.getCuratorClient();
        // 开启会话
        client.start();

        // 第一次创建临时顺序节点
        String s1 = client.create()
                // 如果有父节点会一起创建
                .creatingParentsIfNeeded()
                // 节点类型：临时顺序节点
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                // 节点路径 /wiki
                .forPath("/wiki-");
        // 输出
        System.out.println(s1);

        // 第二次创建临时顺序节点
        String s2 = client.create()
                // 如果有父节点会一起创建
                .creatingParentsIfNeeded()
                // 节点类型：临时顺序节点
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                // 节点路径 /wiki
                .forPath("/wiki-");
        // 输出
        System.out.println(s2);

        // 关闭客户端
        client.close();
    }
}
