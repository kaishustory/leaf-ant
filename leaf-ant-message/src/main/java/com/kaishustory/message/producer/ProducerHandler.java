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

import com.kaishustory.message.common.model.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 消息接收处理
 *
 * @author liguoyang
 * @create 2019-07-29 11:38
 **/
@Slf4j
public class ProducerHandler extends SimpleChannelInboundHandler<RpcResponse> {

    private NettyProducer nettyProducer;

    private CallbackServer callbackServer;

    public ProducerHandler(NettyProducer nettyProducer, CallbackServer callbackServer) {
        this.callbackServer = callbackServer;
        this.nettyProducer = nettyProducer;
    }

    /**
     * 处理服务端返回的数据
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse response) throws Exception {
        try {
            log.info("server callback：{}" , response.toString());
            // 回复事件
            callbackServer.reply(response.getId(), response);
        }catch (Exception e){
            log.error("server callback error ", e);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }

    /**
     * 连接中断事件
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("服务连接中断！");
        super.channelInactive(ctx);
        // 尝试重连服务
        nettyProducer.createClient(nettyProducer.getHost(), nettyProducer.getPort(), 1);
    }
}
