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

package com.kaishustory.leafant.mapping.service;

import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponseCollection;
import com.kaishustory.message.producer.MessageCallback;
import com.kaishustory.message.producer.NettyTopicProducer;
import com.kaishustory.message.producer.Stats;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 同步配置
 * @author liguoyang
 * @create 2019-08-06 15:22
 **/
@Slf4j
@Component
@ConditionalOnProperty(name = "message.mapping.producer", havingValue = "true")
public class MappingSyncService {

    /**
     * 消息生产者
     */
    @Resource(name = "syncMessageProducerObject")
    private NettyTopicProducer syncMessageProducer;

    /**
     * 同步配置
     * @param type 配置类型（TYPE_MQ、TYPE_ES、TYPE_REDIS）
     * @return 是否同步成功
     */
    public boolean sync(String type){

        // 广播配置同步消息
        RpcResponseCollection responseCollection = syncMessageProducer.sendSyncBroadcastMsg(new RpcRequest("sync", type));
        // 返回同步结果
        if(responseCollection.success()) {
            log.info("同步配置成功。type：{}，totalNode：{}，successNode：{}", type, responseCollection.getConsumerSize(), responseCollection.getSuccessSize());
            return true;
        }else {
            log.info("同步配置失败。type：{}，totalNode：{}，successNode：{}", type, responseCollection.getConsumerSize(), responseCollection.getSuccessSize());
            return false;
        }
    }
}
