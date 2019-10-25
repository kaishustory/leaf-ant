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

import com.kaishustory.message.common.model.RpcDecoder;
import com.kaishustory.message.common.model.RpcEncoder;
import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;

/**
 * 消息接收客户端
 *
 * @author liguoyang
 * @create 2019-07-29 11:36
 **/
@Slf4j
public class NettyProducer {

    /**
     * 消息通道
     */
    private Channel channel;

    /**
     * 指令分组
     */
    private EventLoopGroup group;

    /**
     * 回调处理
     */
    private CallbackServer callbackServer;

    /**
     * 远程服务端地址
     */
    private String host;

    /**
     * 远程服务端端口
     */
    private int port;

    /**
     * 当前连接是否畅通
     */
    private boolean connected = false;

    /**
     * 是否销毁连接
     */
    private boolean destroy = false;

    /**
     * 连接服务端的端口号地址和端口
     */
    public NettyProducer(String host, int port) throws ConnectException {
        this.callbackServer = new CallbackServer();
        this.host = host;
        this.port = port;
        createClient(host, port);
    }

    /**
     * 创建连接
     *
     * @param host 地址
     * @param port 端口
     * @throws ConnectException
     */
    protected void createClient(String host, int port) throws ConnectException {
        createClient(host, port, 0);
    }

    /**
     * 创建连接
     *
     * @param host        地址
     * @param port        端口
     * @param reconnCount 重连次数
     * @throws ConnectException
     */
    protected void createClient(String host, int port, int reconnCount) throws ConnectException {
        try {
            if (destroy) {
                log.info("连接已被销毁！addr：{}:{}", host, port);
                return;
            }
            group = new NioEventLoopGroup();

            NettyProducer nettyProducer = this;
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)  // 使用NioSocketChannel来作为连接用的channel类
                    .handler(new ChannelInitializer<SocketChannel>() { // 绑定连接初始化器
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            log.info("正在连接中... addr：{}:{}", host, port);
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new RpcEncoder(RpcRequest.class)); //编码request
                            pipeline.addLast(new RpcDecoder(RpcResponse.class)); //解码response
                            pipeline.addLast(new ProducerHandler(nettyProducer, callbackServer)); //客户端处理类

                        }
                    });
            //发起异步连接请求，绑定连接端口和host信息
            final ChannelFuture future = b.connect(host, port).sync();

            future.addListener((ChannelFutureListener) listener -> {
                if (future.isSuccess()) {
                    connected = true;
                    log.info("连接服务器成功 addr：{}:{}", host, port);

                } else {
                    connected = false;
                    log.error("连接服务器失败 addr：{}:{}", host, port, future.cause());
                    //关闭线程组
                    group.shutdownGracefully().sync();
                }
            });

            this.channel = future.channel();

        } catch (Exception t) {
            log.error("创建服务器连接发生异常！addr：{}:{}", host, port, t);
            try {
                //关闭线程组
                group.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            connected = false;
            if (reconnCount <= 180) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
                // 1秒后重新连接
                log.info("尝试重连服务器 addr：{}:{}", host, port);
                // 重新创建连接
                createClient(host, port, ++reconnCount);
            } else {
                throw new ConnectTimeoutException("连接远程消息服务失败！");
            }
        }
    }

    /**
     * 发送消息
     *
     * @param request 消息
     * @return 发送成功
     */
    public boolean send(RpcRequest request) {
        try {
            if (connected) {
                channel.writeAndFlush(request);
                log.info("send msg. host：{}, port：{}，request：{}", host, port, request);
                return true;
            } else {
                log.error("client is suspend. host：{}, port：{}，request：{}", host, port, request);
                return false;
            }
        } catch (Exception e) {
            log.error("send error. host：{}, port：{}，request：{}", host, port, request, e);
            return false;
        }
    }

    /**
     * 发送消息（回复处理）
     *
     * @param request  消息
     * @param callback 消息回复处理
     * @return 发送成功
     */
    public boolean sendCallback(RpcRequest request, MessageCallback callback) {
        try {
            if (connected) {
                channel.writeAndFlush(request);
                // 注册回调事件
                callbackServer.register(request.getMsgId(), callback);
                log.info("send msg. host：{}, port：{}，request：{}", host, port, request);
                return true;
            } else {
                log.error("client is suspend. host：{}, port：{}，request：{}", host, port, request);
                return false;
            }
        } catch (Exception e) {
            log.error("send error. host：{}, port：{}，request：{}", host, port, request, e);
            return false;
        }
    }

    /**
     * 销毁连接
     */
    public void destroy() {
        destroy = true;
        if (channel != null && channel.isOpen()) {
            // 关闭通道
            channel.close();
        }
        if (group != null) {
            try {
                //关闭线程组
                group.shutdownGracefully().sync();
            } catch (InterruptedException e) {
            }
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isDestroy() {
        return destroy;
    }

    public String getAddr() {
        return host + ":" + port;
    }
}
