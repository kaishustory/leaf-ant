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

package com.kaishustory.leafant.transform.redis.service;

import com.kaishustory.leafant.common.constants.EventConstants;
import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.transform.redis.dao.RedisDao;
import com.kaishustory.leafant.transform.redis.model.RedisEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Redis结构转换服务
 *
 * @author liguoyang
 * @create 2019-08-20 16:35
 **/
@Service
public class RedisTransformService {

    /**
     * Redis处理
     */
    @Autowired
    private RedisDao redisDao;

    /**
     * Redis 同步事件处理
     *
     * @param redisDataSourceConfig redis数据源配置
     * @param redisEvents           Redis事件列表
     */
    public void eventHandle(RedisSyncConfig.RedisDataSourceConfig redisDataSourceConfig, List<RedisEvent> redisEvents) {

        // 批量处理
        redisDao.batch(redisDataSourceConfig, redisEvents, (RedisDao.RedisHandle<RedisEvent>) (connection, event) -> {
                    switch (event.getType()) {
                        // 新增、更新，写入数据
                        case EventConstants.TYPE_INSERT:
                        case EventConstants.TYPE_UPDATE:
                            connection.set(event.getRedisKey().getBytes(), event.getBody().getBytes());
                            Log.info("【Redis】更新文档 key：{}，delay：{}，body：{}", event.getRedisKey(), (System.currentTimeMillis() - event.getExecuteTime()) + "/ms", event.getBody());
                            break;

                        // 删除，删除数据
                        case EventConstants.TYPE_DELETE:
                            connection.del(event.getRedisKey().getBytes());
                            Log.info("【Redis】删除文档 key：{}，delay：{}", event.getRedisKey(), (System.currentTimeMillis() - event.getExecuteTime()) + "/ms");
                            break;

                        default:
                    }
                }
        );

    }


}
