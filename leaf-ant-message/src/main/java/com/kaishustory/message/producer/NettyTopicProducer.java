/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.kaishustory.message.producer;

import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponse;
import com.kaishustory.message.common.model.RpcResponseCollection;
import com.kaishustory.message.common.zookeeper.ZooOpera;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.kaishustory.message.common.constants.MessageConstants.GROUP_DEFAULT;

/**
 * Netty 按主题发送消息
 *
 * @author liguoyang
 * @create 2019-07-30 15:05
 **/
@Slf4j
public class NettyTopicProducer {

    /**
     * 分组
     */
    private String group;

    /**
     * 主题
     */
    private String topic;

    /**
     * Zookeeper地址
     */
    private String zoo_addr;

    /**
     * 是否关闭
     */
    private boolean close = false;

    /**
     * 生产者列表锁
     */
    private ReadWriteLock consumerListLock = new ReentrantReadWriteLock();

    /**
     * 生产者列表（一对一对应消费者）
     */
    private List<NettyProducer> consumerList = new ArrayList<>();

    /**
     * Netty 按主题发送消息
     *
     * @param group    分组
     * @param topic    主题
     * @param zoo_addr Zookeeper地址
     */
    public NettyTopicProducer(String group, String topic, String zoo_addr) {
        this.group = group;
        this.topic = topic;
        this.zoo_addr = zoo_addr;

        // 更新消费者列表
        updateConsumerList();

        // 每5秒更新消费者列表
        new Thread(() -> {
            while (!close) {
                try {
                    try {
                        Thread.sleep(1000 * 5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // 更新消费者列表
                    updateConsumerList();
                } catch (Throwable t) {
                    log.error("更新netty消费者列表失败！Group：{} Topic：{}", group, topic);
                }
            }
        }, String.format("netty-producer-consumerlist-refresh-%s-%s", group, topic)).start();
    }

    /**
     * Netty 按主题发送消息
     *
     * @param topic    主题
     * @param zoo_addr Zookeeper地址
     */
    public NettyTopicProducer(String topic, String zoo_addr) {
        this(GROUP_DEFAULT, topic, zoo_addr);
    }

    /**
     * 发送单播消息
     *
     * @param request 消息内容
     */
    public Stats sendMsg(RpcRequest request) {
        List<Stats.ConsumerStats> consumerStatsList = new ArrayList<>(consumerList.size());
        Stats stats = new Stats(request.getMsgId(), group, topic, consumerStatsList);
        try {
            consumerListLock.readLock().lock();
            NettyProducer client = getOneClient(request);
            if (client != null) {
                boolean success = client.send(request);
                consumerStatsList.add(new Stats.ConsumerStats(client.getAddr(), success));
            } else {
                log.info("no consumer msg is ignore. group：{} topic：{}，request：{}", group, topic, request);
            }
        } finally {
            consumerListLock.readLock().unlock();
        }
        return stats;
    }

    /**
     * 同步消息（默认：超时时间30秒）
     *
     * @param request 请求信息
     * @return 响应信息
     */
    public RpcResponse sendSyncMsg(RpcRequest request) {
        return sendSyncMsg(request, 30, TimeUnit.SECONDS);
    }

    /**
     * 同步消息
     *
     * @param request  请求信息
     * @param timeout  超时
     * @param timeUnit 超时单位
     * @return 响应信息
     */
    public RpcResponse sendSyncMsg(RpcRequest request, long timeout, TimeUnit timeUnit) {
        CountDownLatch count = new CountDownLatch(1);
        AtomicReference<RpcResponse> referenceRef = new AtomicReference<>();
        // 发送消息
        Stats stats = sendMsg(request, response -> {
            referenceRef.set(response);
            count.countDown();
        });
        if (stats.success()) {
            try {
                // 等待返回
                count.await(timeout, timeUnit);
            } catch (InterruptedException e) {
                return RpcResponse.errorResponse("message callback timeout!");
            }
            return referenceRef.get();
        } else {
            return RpcResponse.errorResponse("message send fail!");
        }
    }

    /**
     * 发送单播消息
     *
     * @param request  消息内容
     * @param callback 回复处理
     */
    public Stats sendMsg(RpcRequest request, MessageCallback callback) {
        List<Stats.ConsumerStats> consumerStatsList = new ArrayList<>(consumerList.size());
        Stats stats = new Stats(request.getMsgId(), group, topic, consumerStatsList);
        try {
            consumerListLock.readLock().lock();
            NettyProducer client = getOneClient(request);
            if (client != null) {
                boolean success = client.sendCallback(request, callback);
                consumerStatsList.add(new Stats.ConsumerStats(client.getAddr(), success));
            } else {
                log.info("no consumer msg is ignore. group：{} topic：{}，request：{}", group, topic, request);
            }
        } finally {
            consumerListLock.readLock().unlock();
        }
        return stats;
    }

    /**
     * 发送广播消息
     *
     * @param request 消息内容
     * @return 返回结果
     */
    public Stats sendBroadcastMsg(RpcRequest request) {
        List<Stats.ConsumerStats> consumerStatsList = new ArrayList<>(consumerList.size());
        Stats stats = new Stats(request.getMsgId(), group, topic, consumerStatsList);
        try {
            consumerListLock.readLock().lock();
            List<NettyProducer> openClientList = getOpenClientList();
            if (openClientList.size() > 0) {
                for (NettyProducer client : openClientList) {
                    boolean success = client.send(request);
                    consumerStatsList.add(new Stats.ConsumerStats(client.getAddr(), success));
                }
            } else {
                log.info("no consumer msg is ignore. group：{} topic：{}，request：{}", group, topic, request);
            }
        } finally {
            consumerListLock.readLock().unlock();
        }
        return stats;
    }

    /**
     * 发送同步广播消息（默认：超时时间30秒）
     *
     * @param request 消息内容
     * @return 返回结果
     */
    public RpcResponseCollection sendSyncBroadcastMsg(RpcRequest request) {
        return sendSyncBroadcastMsg(request, 30, TimeUnit.SECONDS);
    }

    /**
     * 发送同步广播消息
     *
     * @param request  消息内容
     * @param timeout  超时
     * @param timeUnit 超时单位
     * @return 返回结果
     */
    public RpcResponseCollection sendSyncBroadcastMsg(RpcRequest request, long timeout, TimeUnit timeUnit) {
        long start = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger();
        List<RpcResponse> responses = new ArrayList<>();

        // 发送广播消息
        Stats stats = sendBroadcastMsg(request, (response -> {
            responses.add(response);
            count.incrementAndGet();
        }));

        int consumerSize = stats.getConsumerStats().size();
        while (count.intValue() < consumerSize) {
            // 超时检查
            if (System.currentTimeMillis() - start > getMillis(timeout, timeUnit)) {
                return new RpcResponseCollection(consumerSize, responses, "message callback timeout!");
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }
        // 返回结果
        return new RpcResponseCollection(consumerSize, responses);
    }

    /**
     * 单位转换为毫秒
     *
     * @param timeout  超时
     * @param timeUnit 超时单位
     * @return 超时毫秒数
     */
    private long getMillis(long timeout, TimeUnit timeUnit) {
        switch (timeUnit) {
            case DAYS:
                return 1000 * 60 * 60 * 24 * timeout;
            case HOURS:
                return 1000 * 60 * 60 * timeout;
            case MINUTES:
                return 1000 * 60 * timeout;
            case SECONDS:
                return 1000 * timeout;
            case MILLISECONDS:
                return timeout;
            default:
                return timeout;
        }
    }

    /**
     * 发送广播消息
     *
     * @param request  消息内容
     * @param callback 回复处理
     */
    public Stats sendBroadcastMsg(RpcRequest request, MessageCallback callback) {
        List<Stats.ConsumerStats> consumerStatsList = new ArrayList<>(consumerList.size());
        Stats stats = new Stats(request.getMsgId(), group, topic, consumerStatsList);
        try {
            consumerListLock.readLock().lock();
            List<NettyProducer> openClientList = getOpenClientList();
            if (openClientList.size() > 0) {
                for (NettyProducer client : openClientList) {
                    boolean success = client.sendCallback(request, callback);
                    consumerStatsList.add(new Stats.ConsumerStats(client.getAddr(), success));
                }
            } else {
                log.info("no consumer msg is ignore. group：{}，topic：{}，request：{}", group, topic, request);
            }
        } finally {
            consumerListLock.readLock().unlock();
        }
        return stats;
    }


    /**
     * 更新消费者列表
     */
    private synchronized void updateConsumerList() {

        if (close) {
            return;
        }

        ZooOpera zooOpera = new ZooOpera(zoo_addr);
        // 服务器有效的，消费者列表
        List<String> serverAddrList = zooOpera.getCousumerList(group, topic);

        // 当前连接的，消费者列表
        Set<String> currAddr = consumerList.stream().map(NettyProducer::getAddr).collect(Collectors.toSet());

        // 新增可用的，消费者列表
        List<String> addAddr = serverAddrList.stream().filter(addr -> !currAddr.contains(addr)).collect(Collectors.toList());
        // 已失效的，消费者列表
        List<String> invalidAddr = currAddr.stream().filter(addr -> !serverAddrList.contains(addr)).collect(Collectors.toList());

        if (addAddr.size() > 0) {
            List<NettyProducer> nettyProducerList = addAddr.stream().map(addr -> addr.split(":")).filter(addrs -> addrs.length > 0 && addrs.length <= 2).map(addrs -> {
                try {

                    return new NettyProducer(addrs[0], addrs.length == 1 ? 80 : Integer.parseInt(addrs[1]));
                } catch (Exception e) {
                    log.error("创建连接时发生异常！addr: {}", addrs, e);
                }
                return null;

            }).filter(Objects::nonNull).collect(Collectors.toList());

            try {
                consumerListLock.writeLock().lock();
                consumerList.addAll(nettyProducerList);
                log.info("新增消费者：Group：{}，Topic：{}, Consumer：{}", group, topic, nettyProducerList.stream().map(client -> client.getHost() + ":" + client.getPort()).collect(Collectors.toList()));
            } finally {
                consumerListLock.writeLock().unlock();
            }

        }

        if (invalidAddr.size() > 0) {
            List<NettyProducer> destroyList = consumerList.stream().filter(client -> invalidAddr.contains(client.getAddr())).collect(Collectors.toList());
            destroyList.forEach(NettyProducer::destroy);
            try {
                consumerListLock.writeLock().lock();
                consumerList.removeAll(destroyList);
                log.info("删除消费者：Group：{}，Topic：{}, Consumer：{}", group, topic, invalidAddr);
            } finally {
                consumerListLock.writeLock().unlock();
            }
        }
    }

    /**
     * 获得一个服务器连接
     *
     * @param request
     * @return
     */
    private NettyProducer getOneClient(RpcRequest request) {
        List<NettyProducer> openClientList = getOpenClientList();
        if (openClientList.size() > 0) {
            int i = Math.abs(Objects.hashCode(request.getMsgId())) % openClientList.size();
            return openClientList.get(i);
        } else {
            return null;
        }
    }

    /**
     * 获得有效连接列表
     */
    private List<NettyProducer> getOpenClientList() {
        return consumerList.stream().filter(NettyProducer::isConnected).collect(Collectors.toList());
    }

    /**
     * 关闭所有连接
     */
    public void close() {
        close = true;
        consumerList.forEach(NettyProducer::destroy);
        consumerList.clear();
    }
}
