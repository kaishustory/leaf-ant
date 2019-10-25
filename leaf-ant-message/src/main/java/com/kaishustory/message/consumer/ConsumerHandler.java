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

import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;


/**
 * 消息处理
 **/
@Slf4j
public class ConsumerHandler extends ChannelInboundHandlerAdapter {

    private MessageHandler messageHandler;

    public ConsumerHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    /**
     * 接受client发送的消息
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        RpcRequest request = (RpcRequest) msg;
        log.info("client msg：" + request.toString());
        // 消息处理
        RpcResponse response = messageHandler.handler(request);
        if (response != null && response.getStatus() != 0) {
            // 回复消息，使用请求时的ID
            response.setId(request.getMsgId());
            // 回复消息
            ctx.writeAndFlush(response);
        }
    }

    /**
     * 通知处理器最后的channelRead()是当前批处理中的最后一条消息时调用
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        System.out.println("服务端接收数据完毕..");
        ctx.flush();
    }

    //读操作时捕获到异常时调用
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    //客户端去和服务端连接成功时触发
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        ctx.writeAndFlush("hello client");
    }

}
