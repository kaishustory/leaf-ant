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
import com.kaishustory.leafant.common.model.EsSyncConfig;
import com.kaishustory.leafant.common.model.EsSyncMappingTable;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.common.utils.Time;
import com.kaishustory.leafant.mapping.dao.EsMappingDao;
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
 * ElasticSearch映射缓存
 **/
@Component
public class EsMappingCache {

    /**
     * ES映射表格缓存
     */
    private Cache<String, List<EsSyncMappingTable>> esMappingTableCache = Caffeine.newBuilder().build();

    /**
     * ES映射配置缓存
     */
    private Cache<String, EsSyncConfig> esMappingConfigCache = Caffeine.newBuilder().build();

    /**
     * 缓存锁
     */
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * 映射Dao
     */
    @Autowired
    private EsMappingDao mappingDao;

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
     * 加载ES映射配置
     *
     * @param init 系统启动加载
     */
    public void loadMapping(boolean init) {
        Time time = new Time("Mongo加载ES映射配置");
        try {
            if (init) {
                readWriteLock.writeLock().lock();
            }
            // 查询全部有效映射表 <RDS:DATABASE:TABLE, MappingTable>
            Map<String, List<EsSyncMappingTable>> allMappingTable = mappingDao.findSyncMapping(filterSync).stream().collect(Collectors.groupingBy(m -> getCacheKey(m.getSourceRds(), m.getSourceDatabase(), m.getSourceTable())));

            // 查询全部映射配置 <ID, MappingConfig>
            Map<String, EsSyncConfig> allConfig = mappingDao.findAllMapping().stream().collect(Collectors.toMap(EsSyncConfig::getId, m -> m));

            if (!init) {
                readWriteLock.writeLock().lock();
            }

            esMappingTableCache.invalidateAll();
            esMappingConfigCache.invalidateAll();
            esMappingTableCache.putAll(allMappingTable);
            esMappingConfigCache.putAll(allConfig);

        } catch (Throwable t) {
            Log.info("ES配置加载发生异常！", t);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        time.end();
    }

    /**
     * 读取ES映射结构列表
     *
     * @param mappingId 映射ID（仅同步事件使用）
     * @param table     表
     * @return ES映射结构列表
     */
    public Option<List<EsSyncMappingTable>> getMapping(String mappingId, String table) {
        try {
            readWriteLock.readLock().lock();

            EsSyncConfig esSyncConfig = esMappingConfigCache.getIfPresent(mappingId);
            if (esSyncConfig != null && (esSyncConfig.isSync() || !filterSync)) {
                return Option.of(esSyncConfig.getTableList().stream().filter(tableInfo -> tableInfo.getSourceTable().equals(table)).collect(Collectors.toList()));
            } else {
                return Option.empty();
            }

        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * 读取ES映射结构列表
     *
     * @param rds      实例
     * @param database 数据库
     * @param table    表
     * @return ES映射结构列表
     */
    public Option<List<EsSyncMappingTable>> getMapping(String rds, String database, String table) {
        try {
            readWriteLock.readLock().lock();
            List<EsSyncMappingTable> mapping = esMappingTableCache.getIfPresent(getCacheKey(rds, database, table));
            if (mapping != null && mapping.size() > 0) {
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
