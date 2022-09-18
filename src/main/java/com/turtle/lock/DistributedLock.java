package com.turtle.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {
    // zookeeper server 列表
    private static String connectString = "175.178.69.88:2181,175.178.69.88:2182,175.178.69.88:2183";
    // 超时时间
    private int sessionTimeout = 200000;
    private ZooKeeper zk;
    private String rootNode = "locks";
    private String subNode = "seq-";
    // 当前 client 等待的子节点
    private String waitPath;
    //ZooKeeper 连接
    // CountDownLatch:允许一个或多个线程等待，直到在其他线程中执行的一组操作完成
    private CountDownLatch connectLatch = new CountDownLatch(1);
    //ZooKeeper 节点等待
    private CountDownLatch waitLatch = new CountDownLatch(1);
    // 当前 client 创建的子节点
    private String currentNode;

    // 和 zk 服务建立连接，并创建根节点
    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 	客户端与服务器正常连接时, 打开 latch, 唤醒 wait在该 latch 上的线程
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    // 使latch的值减1，如果减到了0，则会唤醒所有等待在这个latch上的线程。
                    // 连接建立成功，可以继续后续的新建根节点的操作
                    connectLatch.countDown();
                }
                // 发生了 waitPath 的删除事件
                if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });
        // 等待连接建立，需要建立连接完成后才进行后续的新增根节点的操作
        // 使当前线程进入同步队列进行等待，直到latch的值被减到0或者当前线程被中断，当前线程就会被唤醒。
        connectLatch.await();
        //获取根节点状态
        Stat stat = zk.exists("/" + rootNode, false);
        //如果根节点不存在，则创建根节点，根节点类型为永久节点
        if (stat == null) {
            System.out.println("根节点不存在");
            // ZooDefs.Ids.OPEN_ACL_UNSAFE：所有可见
            // CreateMode.PERSISTENT：永久存储
            zk.create("/" + rootNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 加锁方法
    public void zkLock() {
        try {
            //在根节点下创建临时顺序节点，返回值为创建的节点路径
            // CreateMode.EPHEMERAL_SEQUENTIAL：临时顺序编号目录节点
            currentNode = zk.create("/" + rootNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            // wait 一小会, 让结果更清晰一些
            Thread.sleep(10);
            // 注意, 没有必要监听"/locks"的子节点的变化情况
            List<String> childrenNodes = zk.getChildren("/" + rootNode, false);
            // 列表中只有一个子节点, 那肯定就是 currentNode , 说明client 获得锁
            if (childrenNodes.size() == 1) {
                return;
            } else {
                //对根节点下的所有临时顺序节点进行从小到大排序
                Collections.sort(childrenNodes);
                //当前节点名称
                // /locks/seq-000000001     seq-000000001：节点名称
                String thisNode = currentNode.substring(("/" + rootNode + "/").length());
                //获取当前节点的位置
                int index = childrenNodes.indexOf(thisNode);
                if (index == -1) {
                    System.out.println("数据异常");
                } else if (index == 0) {
                    // index == 0, 说明 thisNode 在列表中最小, 当前client 获得锁

                    return;
                } else {
                    // 获得排名比 currentNode 前 1 位的节点
                    this.waitPath = "/" + rootNode + "/" + childrenNodes.get(index - 1);
                    // 在 waitPath 上注册监听器, 当 waitPath 被删除时,zookeeper 会回调监听器的 process 方法
                    zk.getData(waitPath, true, new Stat());
                    //进入等待锁状态
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

    // 解锁方法
    public void zkUnlock() {
        try {
            zk.delete(this.currentNode, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}