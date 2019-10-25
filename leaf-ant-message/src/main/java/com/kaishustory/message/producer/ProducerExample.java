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
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;

/**
 * 客户端启动
 *
 * @author liguoyang
 * @create 2019-07-29 11:39
 **/
@Slf4j
public class ProducerExample {

    public static void main(String[] args) throws InterruptedException, ConnectException {
        NettyTopicProducer topicProducer = new NettyTopicProducer("common", "test", "172.16.8.100:2182");
        while (true) {
            // 单播消息
            topicProducer.sendMsg(new RpcRequest("say", "你好"));
            // 单播回复消息
            topicProducer.sendMsg(new RpcRequest("say", "您好"), response -> log.info("回复消息：{}", response));
            // 广播消息
            topicProducer.sendBroadcastMsg(new RpcRequest("say", "世界好"));
            // 广播回复消息
            topicProducer.sendBroadcastMsg(new RpcRequest("say", "大家好"), new MessageCallback() {
                @Override
                public void callback(RpcResponse response) {
                    log.info("回复消息：{}", response);
                }
            });
            Thread.sleep(1000);
        }
    }
}
