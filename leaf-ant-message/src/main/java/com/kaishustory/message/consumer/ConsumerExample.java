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


import com.kaishustory.message.common.model.RpcResponse;
import lombok.extern.slf4j.Slf4j;

/**
 * @author liguoyang
 * @create 2019-07-29 11:33
 **/
@Slf4j
public class ConsumerExample {

    public static void main(String[] args) {
        NettyConsumer nettyConsumer = new NettyConsumer("common", "test", "172.16.8.100:2182", request -> {
            // 回复消息
            log.info("接收到消息");
            return new RpcResponse("reply", "干啥呢", RpcResponse.STATUS_SUCCESS);
        });
    }

}
