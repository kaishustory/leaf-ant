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

package com.kaishustory.leafant.mapping.listener;

import com.kaishustory.leafant.mapping.cache.EsMappingCache;
import com.kaishustory.leafant.mapping.cache.MqMappingCache;
import com.kaishustory.leafant.mapping.cache.MySQLMappingCache;
import com.kaishustory.leafant.mapping.cache.RedisMappingCache;
import com.kaishustory.message.common.model.RpcResponse;
import com.kaishustory.message.consumer.NettyConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.kaishustory.leafant.common.constants.MappingConstants.*;

/**
 * 同步映射消息
 **/
@Configuration
public class SyncMessageListener {

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
     * ES配置
     */
    @Autowired
    private EsMappingCache esMappingCache;

    /**
     * Redis配置
     */
    @Autowired
    private RedisMappingCache redisMappingCache;

    /**
     * MQ配置
     */
    @Autowired
    private MqMappingCache mqMappingCache;

    /**
     * MySQL配置
     */
    @Autowired
    private MySQLMappingCache mysqlMappingCache;

    /**
     * 配置同步消息处理
     *
     * @return
     */
    @Bean(name = "syncMessageConsumerObject")
    @ConditionalOnProperty(name = "message.mapping.sync", havingValue = "true")
    public NettyConsumer createSyncMessageConsumer() {

        // 监听同步映射配置消息
        NettyConsumer consumer = new NettyConsumer(messageGroup, messageTopic, zookeeper, rpcRequest -> {

            // 加载MQ映射配置
            if (TYPE_MQ.equals(rpcRequest.getData())) {
                mqMappingCache.loadMapping(false);
            }

            // 加载ES映射配置
            if (TYPE_ES.equals(rpcRequest.getData())) {
                esMappingCache.loadMapping(false);
            }

            // 加载Redis映射配置
            if (TYPE_REDIS.equals(rpcRequest.getData())) {
                redisMappingCache.loadMapping(false);
            }

            // 加载Redis映射配置
            if (TYPE_MYSQL.equals(rpcRequest.getData())) {
                mysqlMappingCache.loadMapping(false);
            }
            return new RpcResponse("sync-callback", "ok", RpcResponse.STATUS_SUCCESS);
        });
        return consumer;
    }

}
