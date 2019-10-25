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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.mapping.dao.RedisMappingDao;
import com.kaishustory.leafant.transform.redis.dao.RedisDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Redis查询服务
 **/
@Service
public class RedisQueryService {

    /**
     * Redis映射二级缓存
     */
    private Cache<String, RedisSyncConfig> redisMapping2Cache = Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    /**
     * Redis处理
     */
    @Autowired
    private RedisDao redisDao;

    /**
     * Redis映射配置
     */
    @Autowired
    private RedisMappingDao redisMappingDao;

    /**
     * 批量查询Redis内容
     *
     * @param redisDataSourceConfig redis配置
     * @param keys                  Key列表
     * @return Redis 内容 <RedisKey,列信息>
     */
    private Map<String, EventColumn[]> findKeyValues(RedisSyncConfig.RedisDataSourceConfig redisDataSourceConfig, List<String> keys) {

        // 批量查询
        List<String> values = redisDao.multGet(redisDataSourceConfig, keys);

        Map<String, EventColumn[]> keyValues = new HashMap<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            keyValues.put(keys.get(i), JsonUtils.fromJson(values.get(i), EventColumn[].class));
        }
        return keyValues;
    }

    /**
     * 批量查询Redis内容
     *
     * @param mappingId Redis映射ID
     * @param keys      Key列表
     * @return Redis 内容 <RedisKey,列信息>
     */
    public Map<String, EventColumn[]> findKeyValues(String mappingId, List<String> keys) {
        RedisSyncConfig redisSyncConfig = findById(mappingId);
        return findKeyValues(redisSyncConfig.getRedisDataSourceConfig(), keys);
    }

    /**
     * 查询Redis内容
     *
     * @param redisDataSourceConfig redis配置
     * @param key                   key
     * @return Redis 内容
     */
    public EventColumn[] findValue(RedisSyncConfig.RedisDataSourceConfig redisDataSourceConfig, String key) {
        String value = redisDao.get(redisDataSourceConfig, key);
        if (value != null) {
            // 查询
            return JsonUtils.fromJson(value, EventColumn[].class);
        } else {
            return new EventColumn[0];
        }
    }

    /**
     * 查询Redis内容
     *
     * @param mappingId Redis映射ID
     * @param key       key
     * @return Redis 内容
     */
    public EventColumn[] findValue(String mappingId, String key) {
        // 查询
        return findValue(findById(mappingId).getRedisDataSourceConfig(), key);
    }

    /**
     * 查询Redis映射信息
     *
     * @param mappingId 映射ID
     * @return Redis映射信息
     */
    public RedisSyncConfig findById(String mappingId) {
        RedisSyncConfig redisSyncConfig = redisMapping2Cache.get(mappingId, s -> null);
        if (redisSyncConfig == null) {
            redisSyncConfig = redisMappingDao.findById(mappingId);
            redisMapping2Cache.put(mappingId, redisSyncConfig);
        }
        return redisSyncConfig;
    }
}
