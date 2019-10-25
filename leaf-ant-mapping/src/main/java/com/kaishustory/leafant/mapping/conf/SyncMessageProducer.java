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

package com.kaishustory.leafant.mapping.conf;

import com.kaishustory.message.producer.NettyTopicProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 同步消息发送者
 *
 * @author liguoyang
 * @create 2019-07-30 20:18
 **/
@Configuration
@ConditionalOnProperty(name = "message.mapping.producer", havingValue = "true")
public class SyncMessageProducer {

    /**
     * Zookeeper地址
     */
    @Value("${zookeeper.url}")
    private String zookeeper;

    /**
     * 同步消息Group
     */
    @Value("${message.group}")
    private String messageGroup;

    /**
     * 同步消息Topic
     */
    @Value("${message.topic}")
    private String messageTopic;

    /**
     * 同步消息发送者
     */
    @Bean(name = "syncMessageProducerObject")
    @ConditionalOnProperty(name = "message.mapping.producer", havingValue = "true")
    public NettyTopicProducer createProducer() {
        return new NettyTopicProducer(messageGroup, messageTopic, zookeeper);
    }
}
