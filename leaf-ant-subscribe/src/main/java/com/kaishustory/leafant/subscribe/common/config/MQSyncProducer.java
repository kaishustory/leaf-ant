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

package com.kaishustory.leafant.subscribe.common.config;

import com.kaishustory.leafant.common.utils.Log;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 同步MQ生产者配置
 *
 * @author liguoyang
 * @create 2019-07-10 17:12
 **/
@Configuration
public class MQSyncProducer {

    private final String app = "leafant";

    @Value("${spring.profiles.active}")
    private String env;

    /**
     * 创建MQ生产者
     */
    @Primary
    @Bean("syncMQ")
    public MQProducer createProducer(@Value("${mq.sync.groupId}") String group, @Value("${mq.addr}") String addr) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer(group);
            producer.setNamesrvAddr(addr);
            producer.start();
            return producer;
        } catch (MQClientException e) {
            Log.info("创建同步MQ失败！", e);
            return null;
        }
    }
}
