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

package com.kaishustory.leafant.mapping.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.kaishustory.leafant.common.model.MqSyncConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.common.utils.Time;
import com.kaishustory.leafant.mapping.dao.MqMappingDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * MQ映射缓存
 **/
@Component
public class MqMappingCache {

    /**
     * MQ映射缓存
     */
    private Cache<String, List<MqSyncConfig>> mqMappingCache = Caffeine.newBuilder().build();

    /**
     * 缓存锁
     */
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * 映射Dao
     */
    @Autowired
    private MqMappingDao mappingDao;

    /**
     * 是否过滤未启用配置
     */
    @Value("${message.mapping.filterSync}")
    private boolean filterSync;

    /**
     * 初始化
     */
    @PostConstruct
    public void init() {
        // 加载映射配置
        loadMapping(true);
    }

    /**
     * 加载MQ映射配置
     */
    public void loadMapping(boolean init) {
        Time time = new Time("Mongo加载MQ映射配置");
        try {
            if (init) {
                readWriteLock.writeLock().lock();
            }
            // 查询全部映射数据
            Map<String, List<MqSyncConfig>> allMapping = mappingDao.findAllMapping().stream().collect(Collectors.groupingBy(m -> getCacheKey(m.getSourceRds(), m.getSourceDatabase(), m.getSourceTable())));
            if (!init) {
                readWriteLock.writeLock().lock();
            }
            mqMappingCache.cleanUp();
            mqMappingCache.putAll(allMapping);

        } catch (Throwable t) {
            Log.info("MQ配置加载发生异常！", t);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        time.end();
    }

    /**
     * 读取MQ映射结构列表
     *
     * @param rds      实例
     * @param database 数据库
     * @param table    表
     * @return ES映射结构列表
     */
    public Option<List<MqSyncConfig>> getMapping(String rds, String database, String table) {
        try {
            readWriteLock.readLock().lock();
            List<MqSyncConfig> mapping = mqMappingCache.getIfPresent(getCacheKey(rds, database, table));
            if (mapping != null) {
                return Option.of(mapping);
            } else {
                return Option.empty();
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * 获得缓存KEY
     *
     * @param rds      实例
     * @param database 数据库
     * @param table    表
     * @return 缓存KEY
     */
    private String getCacheKey(String rds, String database, String table) {
        return String.format("%s:%s:%s", rds, database, table);
    }
}
