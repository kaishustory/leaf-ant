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

package com.kaishustory.leafant.transform.common.listener;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.transform.route.EventRouteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

/**
 * 同步MQ消息监听
 *
 * @author liguoyang
 * @create 2019-07-22 14:36
 **/
@Configuration
public class MQSyncListener {

    /**
     * 事件路由
     */
    @Autowired
    private EventRouteService eventRouteService;

    /**
     * 数据同步Topic
     */
    @Value("${mq.sync.topic}")
    private String topic;

    /**
     * 消费线程数
     */
    @Value("${mq.sync.threads:20}")
    private int consumeThreadNums;

    /**
     * 创建MQ消费者
     */
    @Bean
    public Consumer createSyncMqConsumer(@Value("${mq.sync.groupId}") String groupId, @Value("${ons.access-key}") String accessKey, @Value("${ons.secret-key}") String secretKey, @Value("${mq.addr}") String addr){

        Properties properties = new Properties();
        // 您在控制台创建的 Group ID
        properties.put(PropertyKeyConst.GROUP_ID, groupId);
        // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
        properties.put(PropertyKeyConst.AccessKey, accessKey);
        // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
        properties.put(PropertyKeyConst.SecretKey, secretKey);
        // 设置 TCP 接入域名，进入控制台的实例管理页面的“获取接入点信息”区域查看
        properties.put(PropertyKeyConst.NAMESRV_ADDR, addr);
        // 消费线程数
        properties.put(PropertyKeyConst.ConsumeThreadNums, consumeThreadNums);

        Consumer consumer = ONSFactory.createConsumer(properties);
        listening(consumer);
        consumer.start();
        return consumer;
    }

    /**
     * MQ事件监听处理
     * @param consumer 消费者
     */
    private void listening(Consumer consumer){
        consumer.subscribe(topic, "*", (message, context) -> {
            // MQID
            String mqid = message.getUserProperties().getProperty("UNIQ_KEY");
            try {
                String body = new String(message.getBody());
                Event[] events = JsonUtils.fromJson(body, Event[].class);
                Log.info("收到同步MQ消息：{}，Size：{}", mqid, events==null ? 0 : events.length);
                try {
                    eventRouteService.route(events);
                }catch (Exception e){
                    Log.error("MQ消息处理异常，将任务转为单条处理模式！{}", mqid, e);
                    Arrays.stream(Objects.requireNonNull(events)).forEach(event -> {
                        try {
                            eventRouteService.route(event);
                        }catch (Exception e1){
                            Log.error("MQ消息处理异常！{}，table：{}，type：{}，id：{}", mqid, event.getTableKey(), event.getTypeName(), event.getPrimaryKey(), e);
                        }
                    });
                }
                return Action.CommitMessage;
            }catch (Throwable t){
                Log.error("同步MQ消息处理异常！{}", mqid, t);
                return Action.CommitMessage;
            }
        });
    }

}
