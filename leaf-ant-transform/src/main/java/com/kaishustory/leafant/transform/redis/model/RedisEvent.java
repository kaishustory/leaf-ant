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

package com.kaishustory.leafant.transform.redis.model;

import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.utils.JsonUtils;
import lombok.Data;

import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.EventConstants.TYPE_DELETE;

/**
 * Redis事件
 **/
@Data
public class RedisEvent {

    /**
     * 操作类型（1：新增，2：修改，3：删除）
     */
    private int type;

    /**
     * Redis索引KEY
     */
    private String redisKey;

    /**
     * Redis索引内容
     */
    private String body;

    /**
     * 时间发生时间
     */
    private long executeTime;


    /**
     * Redis 事件信息
     *
     * @param event           同步事件
     * @param redisSyncConfig Redis同步配置
     */
    public RedisEvent(Event event, RedisSyncConfig redisSyncConfig) {
        this.type = event.getType();
        this.redisKey = redisSyncConfig.getRedisKeyPrefix() + ":" + getId(event, redisSyncConfig);
        this.body = toBody(event, redisSyncConfig);
        this.executeTime = event.getExecuteTime();
    }

    /**
     * 转换内容
     *
     * @param event           同步事件
     * @param redisSyncConfig Redis同步配置
     * @return 内容
     */
    private String toBody(Event event, RedisSyncConfig redisSyncConfig) {
        if (event.getType() != TYPE_DELETE) {
            if (redisSyncConfig.isSimplifyField()) {
                // 简化结构，只保留 {"字段名称": "字段内容"}
                return JsonUtils.toJson(event.getAfterColumns().stream().collect(Collectors.toMap(EventColumn::getName, EventColumn::getValue)));
            } else {
                // 复杂结构，保留字段类型等信息 [{字段详细信息}]
                return JsonUtils.toJson(event.getAfterColumns());
            }
        } else {
            return null;
        }
    }

    /**
     * 获得主键值
     *
     * @param event           同步事件
     * @param redisSyncConfig Redis同步配置
     * @return 主键值
     */
    private String getId(Event event, RedisSyncConfig redisSyncConfig) {
        // 是否是副本子表，使用关联主表字段作为主键
        if (redisSyncConfig.isCopyChild()) {
            // 使用关联主表字段，作为主键
            return event.getAllColumns().stream().filter(col -> col.getName().equals(redisSyncConfig.getJoinColumn())).map(EventColumn::getValue).findFirst().orElse("");
        } else {
            // 返回主键
            return event.getPrimaryKey();
        }
    }
}
