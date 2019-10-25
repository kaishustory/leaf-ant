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

package com.kaishustory.message.consumer;

import com.kaishustory.message.common.model.RpcDecoder;
import com.kaishustory.message.common.model.RpcEncoder;
import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponse;
import com.kaishustory.message.common.zookeeper.ZooOpera;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import static com.kaishustory.message.common.constants.MessageConstants.GROUP_DEFAULT;


/**
 * 服务端
 **/
@Slf4j
public class NettyConsumer {

    /**
     * 建立服务器
     *
     * @param group          分组
     * @param topic          主题
     * @param port           端口
     * @param zoo_addr       Zookeeper地址
     * @param messageHandler 消息处理
     */
    public NettyConsumer(String group, String topic, int port, String zoo_addr, MessageHandler messageHandler) {
        bind(group, topic, port, zoo_addr, messageHandler);
    }

    /**
     * 建立服务器
     *
     * @param group          分组
     * @param topic          主题
     * @param zoo_addr       Zookeeper地址
     * @param messageHandler 消息处理
     */
    public NettyConsumer(String group, String topic, String zoo_addr, MessageHandler messageHandler) {
        bind(group, topic, AutoPort.get(), zoo_addr, messageHandler);
    }

    /**
     * 建立服务器
     *
     * @param topic          主题
     * @param port           端口
     * @param zoo_addr       Zookeeper地址
     * @param messageHandler 消息处理
     */
    public NettyConsumer(String topic, int port, String zoo_addr, MessageHandler messageHandler) {
        bind(GROUP_DEFAULT, topic, port, zoo_addr, messageHandler);
    }

    /**
     * 建立服务器
     *
     * @param topic          主题
     * @param zoo_addr       Zookeeper地址
     * @param messageHandler 消息处理
     */
    public NettyConsumer(String topic, String zoo_addr, MessageHandler messageHandler) {
        bind(GROUP_DEFAULT, topic, AutoPort.get(), zoo_addr, messageHandler);
    }

    /**
     * 建立服务器
     *
     * @param group          分组
     * @param topic          主题
     * @param port           端口
     * @param messageHandler 消息处理
     * @param zoo_addr       Zookeeper地址
     */
    private void bind(String group, String topic, int port, String zoo_addr, MessageHandler messageHandler) {
        Thread thread = new Thread(() -> {
            try {
                EventLoopGroup bossGroup = new NioEventLoopGroup(); //bossGroup就是parentGroup，是负责处理TCP/IP连接的
                EventLoopGroup workerGroup = new NioEventLoopGroup(); //workerGroup就是childGroup,是负责处理Channel(通道)的I/O事件

                ServerBootstrap sb = new ServerBootstrap();
                sb.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .option(ChannelOption.SO_BACKLOG, 128) //初始化服务端可连接队列,指定了队列的大小128
                        .childOption(ChannelOption.SO_KEEPALIVE, true) //保持长连接
                        .childHandler(new ChannelInitializer<SocketChannel>() {  // 绑定客户端连接时候触发操作
                            @Override
                            protected void initChannel(SocketChannel sh) throws Exception {
                                sh.pipeline()
                                        .addLast(new RpcDecoder(RpcRequest.class)) //解码request
                                        .addLast(new RpcEncoder(RpcResponse.class)) //编码response
                                        .addLast(new ConsumerHandler(messageHandler)); //使用ServerHandler类来处理接收到的消息
                            }
                        });
                //绑定监听端口，调用sync同步阻塞方法等待绑定操作完
                ChannelFuture future = sb.bind(port).sync();

                if (future.isSuccess()) {
                    // 注册消费者地址
                    ZooOpera zooOpera = new ZooOpera(zoo_addr);
                    zooOpera.addCousumer(group, topic, ZooOpera.getLocalHostLANAddress().get().getHostAddress() + ":" + port);

                    log.info("消费端启动成功 Group：{} Topic：{}", group, topic);
                } else {
                    log.error("消费端启动失败 Group：{} Topic：{}", group, topic, future.cause());
                    bossGroup.shutdownGracefully(); //关闭线程组
                    workerGroup.shutdownGracefully();
                }

                //成功绑定到端口之后,给channel增加一个 管道关闭的监听器并同步阻塞,直到channel关闭,线程才会往下执行,结束进程。
                future.channel().closeFuture().sync();
                log.info("消费端关闭 Group：{} Topic：{}", group, topic);
            } catch (Exception e) {
                log.error("消费端启动异常! Group：{} Topic：{}", group, topic, e);
            }
        });
        thread.setName(String.format("netty-consumer-bind-%s-%s", group, topic));
        thread.start();

    }

}
