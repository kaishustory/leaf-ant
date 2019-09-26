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

package com.kaishustory.leafant.transform.mq.service;

import com.aliyun.openservices.ons.api.*;
import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.model.MqSyncConfig;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;

import static com.kaishustory.leafant.common.constants.EventConstants.*;

/**
 * MQ转发处理
 *
 * @author liguoyang
 * @create 2019-08-28 13:51
 **/
@Service
public class MqTransformService {

    /**
     * 转发MQ
     */
    @Resource(name = "forwardTopic")
    private Producer producer;

    /**
     * 应用
     */
    @Value("${spring.application.name}")
    private String app;

    /**
     * 环境
     */
    @Value("${spring.profiles.active}")
    private String env;

    /**
     * MQ事件转发处理
     * @param events MQ事件列表
     */
    public void eventHandle(MqSyncConfig config, List<Event> events){

        if(events!=null && events.size()>0) {

            Event e = events.get(0);
            // 立即处理
            Message msg = new Message(config.getTargetTopic(), String.format("%s:%s", e.getDatabase(), e.getTable()), e.getTableKey(), JsonUtils.toJson(events).getBytes());
            msg.setShardingKey(e.getTableKey());
            producer.sendAsync(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    Log.info("【MQ】事件转发成功 topic：{}，table：{}，MQID：{}", config.getTargetTopic(), e.getTableKey(), msg.getMsgID());
                }

                @Override
                public void onException(OnExceptionContext context) {
                    Log.error("【MQ】事件转发失败 topic：{}，table：{}，MQID：{}", config.getTargetTopic(), e.getTableKey(), msg.getMsgID());
                }
            });
            // 逐条打印日志
            events.forEach(event -> {
                if(TYPE_INSERT == event.getType()){
                    Log.info("【MQ】新增文档 {}, id：{}，delay：{}，insert：{}", event.getTableKey(), event.getPrimaryKey(), (System.currentTimeMillis() - event.getExecuteTime())+"/ms" , JsonUtils.toJson(event.getUpdateColumnsBase()));
                } else if(TYPE_UPDATE == event.getType()){
                    Log.info("【MQ】更新文档 {}, id：{}，delay：{}，update：{}", event.getTableKey(), event.getPrimaryKey(), (System.currentTimeMillis() - event.getExecuteTime())+"/ms" , JsonUtils.toJson(event.getUpdateColumnsBase()));
                } else if(TYPE_DELETE == event.getType()){
                    Log.info("【MQ】删除文档 {}, id：{}，delay：{}", event.getTableKey(), event.getPrimaryKey(), (System.currentTimeMillis() - event.getExecuteTime())+"/ms");
                }
            });

        }

    }
}
