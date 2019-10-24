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

package com.kaishustory.leafant.transform.redis.dao;

import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.transform.common.conf.RedisConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis操作
 *
 * @author liguoyang
 * @create 2019-08-20 17:28
 **/
@Component
public class RedisDao {

    /**
     * 批量执行分页条数
     */
    private final int pageSize = 100;
    /**
     * Redis
     */
    @Autowired
    private RedisConf redisConf;

    /**
     * 读取内容
     *
     * @param redisDataSourceConfig redis配置
     * @param key                   key
     * @return 内容
     */
    public String get(RedisSyncConfig.RedisDataSourceConfig redisDataSourceConfig, String key) {
        // 获得连接
        StringRedisTemplate redisTemplate = redisConf.getConnection(redisDataSourceConfig.getRedisAddr(), redisDataSourceConfig.getPassword(), redisDataSourceConfig.getDatabase());
        return redisTemplate.opsForValue().get(key);
    }

    /**
     * 批量读取内容
     *
     * @param redisDataSourceConfig redis配置
     * @param keys                  key列表
     * @return 内容列表
     */
    public List<String> multGet(RedisSyncConfig.RedisDataSourceConfig redisDataSourceConfig, List<String> keys) {
        // 获得连接
        StringRedisTemplate redisTemplate = redisConf.getConnection(redisDataSourceConfig.getRedisAddr(), redisDataSourceConfig.getPassword(), redisDataSourceConfig.getDatabase());
        return redisTemplate.opsForValue().multiGet(keys);
    }

    /**
     * 单条写入内容
     *
     * @param redisDataSourceConfig redis配置
     * @param key                   key
     * @param value                 value
     */
    public void save(RedisSyncConfig.RedisDataSourceConfig redisDataSourceConfig, String key, String value) {
        // 获得连接
        StringRedisTemplate redisTemplate = redisConf.getConnection(redisDataSourceConfig.getRedisAddr(), redisDataSourceConfig.getPassword(), redisDataSourceConfig.getDatabase());
        redisTemplate.opsForValue().set(key, value);
    }

    /**
     * 批量处理
     *
     * @param redisDataSourceConfig Redis配置
     * @param params                参数列表
     * @param redisHandle           Redis处理
     * @return 执行结果
     */
    public <T> List<Object> batch(RedisSyncConfig.RedisDataSourceConfig redisDataSourceConfig, List<T> params, RedisHandle redisHandle) {
        // 获得连接
        StringRedisTemplate redisTemplate = redisConf.getConnection(redisDataSourceConfig.getRedisAddr(), redisDataSourceConfig.getPassword(), redisDataSourceConfig.getDatabase());
        List<Object> allResults = new ArrayList<>();
        try {
            // 执行批处理
            redisTemplate.execute((RedisCallback<Object>) redisConnection -> {
                long total = params.size();
                // 分页处理
                for (int page = 0; page < Math.ceil(total / (double) pageSize); page++) {
                    redisConnection.openPipeline();
                    params.stream().skip(page * pageSize).limit(pageSize).forEach(param -> {
                        // Redis处理
                        redisHandle.handle(redisConnection, param);
                    });
                    allResults.addAll(redisConnection.closePipeline());
                }
                return null;
            });
//            Log.info("Redis批处理成功！redis：{}，database：{}", redisDataSourceConfig.getRedisAddr(), redisDataSourceConfig.getDatabase());
        } catch (Exception e) {
            Log.error("Redis批处理失败！redis：{}，database：{}", redisDataSourceConfig.getRedisAddr(), redisDataSourceConfig.getDatabase(), e);
        }
        return allResults;
    }

    public interface RedisHandle<T> {
        void handle(RedisConnection connection, T param);
    }
}
