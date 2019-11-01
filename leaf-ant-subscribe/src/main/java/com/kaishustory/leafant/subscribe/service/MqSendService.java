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

package com.kaishustory.leafant.subscribe.service;

import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.MappingConstants.SOURCE_INIT;

/**
 * MQ消息发送
 **/
@Service
public class MqSendService {

    /**
     * 同步MQ生产者
     */
    @Resource(name = "syncMQ")
    private MQProducer syncProducer;

    /**
     * 同步MQ Topic
     */
    @Value("${mq.sync.topic}")
    private String syncTopic;

    /**
     * 初始化MQ生产者
     */
    @Resource(name = "loadMQ")
    private MQProducer loadProducer;

    /**
     * 初始化MQ Topic
     */
    @Value("${mq.load.topic}")
    private String loadTopic;

    /**
     * 批量发送MQ消息
     *
     * @param eventLists 事件列表
     */
    public List<String> send(List<Event> eventLists) {

        if (eventLists == null || eventLists.size() == 0) {
            return new ArrayList<>();
        }

        List<String> mqid = new ArrayList<>();
        // 每条MQ包含记录条数
        int size = 100;
        // 计划发送MQ条数
        int pageSize = (int) Math.ceil(eventLists.size() / (float) size);

        Event simple = eventLists.get(0);

        // 分页发送MQ
        for (int i = 0; i < pageSize; i++) {

            // 分组ID
            // 提取当前MQ，发送记录列表
            List<Event> eventList = eventLists.stream().skip(i * size).limit(size).collect(Collectors.toList());
            try {
                // 首个事件
                Event firstEvent = eventList.get(0);

                // MQ消息
                Message msg = new Message(
                        getTopic(simple.getSource()), // MQ Topic
                        firstEvent.getDatabase() + ":" + firstEvent.getTable(), // Tag 【数据库_表名】
                        firstEvent.getTableKey(),
                        JsonUtils.toJson(eventList).getBytes() // Body 事件JSON
                );
                // 发送消息
                getProducer(simple.getSource()).sendOneway(msg,
                        // Hash分片值【实例_数据库_表名】
                        (mqs, msg1, key) -> {
                            return mqs.get(Math.abs(Objects.hash(key)) % mqs.size());
                        },
                        simple.getTableKey());

//              eventList.forEach(event -> Log.info("MQID：{}，Table：{}，Event：{}，Updated：{}", msg.getMsgID(), event.getTableKey(), event.getTypeName(), event.getUpdateColumnsBase()));
                mqid.add(msg.getProperty("UNIQ_KEY"));
                Log.info("MQ发送成功. Msg：{}", msg);

            } catch (Exception e) {
                Log.error("MQ发送失败. ", e);
                // 撤销MQ发送异常的事件
                eventList.forEach(event -> {
                    Log.warn("MQ发送失败消息。Event：{}", event);
                });
            }
        }
        return mqid;
    }

    /**
     * 获得MQ 发送者
     *
     * @param source 事件来源
     * @return 发送者
     */
    private MQProducer getProducer(String source) {
        return SOURCE_INIT.equals(source) ? loadProducer : syncProducer;
    }

    /**
     * 获得MQ Topic
     *
     * @param source 事件来源
     * @return Topic
     */
    private String getTopic(String source) {
        return SOURCE_INIT.equals(source) ? loadTopic : syncTopic;
    }
}
