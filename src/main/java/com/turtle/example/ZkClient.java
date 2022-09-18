package com.turtle.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * 连接zookeeper的客户端
 */
public class ZkClient {
    // 单体部署
    // private static String connectString = "175.178.69.88:2181";
    // 通过Docker集群连接(2181 2182 2183)
    private static String connectString = "175.178.69.88:2181,175.178.69.88:2182,175.178.69.88:2183";
    private static int sessionTimeout = 400000;
    private ZooKeeper zkClient = null;

    /**
     * 创建连接
     * @throws Exception
     */
    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new
                Watcher() {
                    /**
                     * 受到监听事件的回调函数
                     * @param watchedEvent
                     */
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        // 收到事件通知后的回调函数（用户的业务逻辑）
                        System.out.println("连接成功：" + watchedEvent.getType() + "--" + watchedEvent.getPath());
                        // 再次启动监听
                        try {
                            // 获取 / 目录下的子节点
                            List<String> children = zkClient.getChildren("/", true);
                            for (String child : children) {
                                System.out.println("子节点为:" + child);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
    }

    /**
     * 测试创建节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        // 参数 1：要创建的节点的路径； 参数 2：节点数据 ； 参数 3：节点权限 ；参数 4：节点的类型
        String nodeCreated = zkClient.create("/atguigu", "测试数据".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建的节点为：" + nodeCreated);
    }

    /**
     * 测试获取子节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println("子节点为:" + child);
        }
        // 延时，方便看出监听状态
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 测试判断是否存在该节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void exist() throws KeeperException, InterruptedException {
        // 判断是否存在对应节点
        Stat stat = zkClient.exists("/atguigu", false);
        System.out.println(stat == null ? "not exist " : "exist");
    }
}
