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

import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.transform.route.EventRouteService;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
     * 消费线程数
     */
    @Value("${mq.sync.threads:20}")
    private int consumeThreadNums;

    /**
     * 创建MQ消费者
     */
    @Bean
    public MQPushConsumer createSyncMqConsumer(@Value("${mq.sync.groupId}") String group, @Value("${mq.sync.topic}") String topic, @Value("${mq.addr}") String addr) {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        try {
            consumer.setNamesrvAddr(addr);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.subscribe(topic, "*");
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    msgs.forEach(message -> {
                        // MQID
                        String mqid = message.getProperty("UNIQ_KEY");
                        try {
                            String body = new String(message.getBody());
                            Event[] events = JsonUtils.fromJson(body, Event[].class);
                            Log.info("收到同步MQ消息：{}，Size：{}", mqid, events == null ? 0 : events.length);
                            try {
                                eventRouteService.route(events);
                            } catch (Exception e) {
                                Log.error(String.format("MQ消息处理异常，将任务转为单条处理模式！%s", mqid), e);
                                Arrays.stream(Objects.requireNonNull(events)).forEach(event -> {
                                    try {
                                        eventRouteService.route(event);
                                    } catch (Exception e1) {
                                        Log.error(String.format("MQ消息处理异常！%s，table：%s，type：%s，id：%s", mqid, event.getTableKey(), event.getTypeName(), event.getPrimaryKey()), e1);
                                    }
                                });
                            }
                        } catch (Throwable t) {
                            Log.error("同步MQ消息处理异常！{}", mqid, t);
                        }
                    });
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            Log.error("创建MQ监听失败！", e);
        }
        return consumer;
    }


}
