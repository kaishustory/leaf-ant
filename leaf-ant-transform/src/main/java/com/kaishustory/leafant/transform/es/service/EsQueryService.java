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

package com.kaishustory.leafant.transform.es.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.gson.JsonObject;
import com.kaishustory.leafant.common.model.EsSyncMappingField;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Time;
import com.kaishustory.leafant.transform.es.dao.ElasticSearchDao;
import com.kaishustory.leafant.transform.es.model.ChildQueryInfo;
import com.kaishustory.leafant.transform.redis.dao.RedisDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ES查询服务
 **/
@Service
public class EsQueryService {

    /**
     * ES处理
     */
    @Autowired
    private ElasticSearchDao elasticSearchDao;

    /**
     * Redis处理
     */
    @Autowired
    private RedisDao redisDao;

    /**
     * Redis地址
     */
    @Value("${redis.url}")
    private String redisAddr;

    /**
     * Redis密码
     */
    @Value("${redis.password}")
    private String redisPassword;

    /**
     * Redis数据库
     */
    @Value("${redis.database}")
    private int redisDatabase;

    /**
     * ES事件缓存
     */
    private Cache<String, Map<String, String>> esEventCache = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).maximumSize(5000).build();

    /**
     * ES不存在缓存
     */
    private Cache<String, String> emptyEventCache = Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS).maximumSize(5000).build();

    /**
     * 批量查询ES 子表内容
     *
     * @param childQueryInfoList 查询ID列表
     * @return ES 内容 <ID,列信息>
     */
    public Map<String, Map<String, String>> findKeyValues(List<ChildQueryInfo> childQueryInfoList) {

        if (childQueryInfoList.size() > 0) {
            Map<String, Map<String, String>> keyValues = new HashMap<>(childQueryInfoList.size());

            // 查询一遍缓存，过滤缓存命中记录
            List<ChildQueryInfo> noCacheQueryList = findCacheQuery(childQueryInfoList, keyValues);

            if (noCacheQueryList.size() > 0) {
                Time time = new Time("【ES】从ES批量查询子表数据。");
                Log.info("【ES】从ES批量查询子表数据。esAddr：{}，index：{}，ids：{}", noCacheQueryList.get(0).getEsAddr(), noCacheQueryList.get(0).getEsIndex(), noCacheQueryList.stream().map(ChildQueryInfo::getEsQueryId).distinct().reduce((a, b) -> a + "," + b).orElse(""));
                // 批量查询
                List<JsonObject> values = elasticSearchDao.multChildQuery(noCacheQueryList.get(0).getEsAddr(), noCacheQueryList.get(0).getEsIndex(), noCacheQueryList);

                Map<String, Map<String, String>> cacheList = new HashMap<>();
                List<String> noCacheList = new ArrayList<>();
                for (int i = 0; i < noCacheQueryList.size(); i++) {
                    ChildQueryInfo queryInfo = noCacheQueryList.get(i);
                    JsonObject doc = values.get(i);
                    if (doc != null) {
                        // 提取字段列表
                        Map<String, String> eventColumns = queryInfo.getMappingTable().getFieldMapping().stream()
                                .collect(Collectors.toMap(EsSyncMappingField::getField, f -> doc.has(f.getField()) ? doc.get(f.getField()).getAsString() : ""));
                        // 返回数据
                        keyValues.put(queryInfo.getEsQueryId(), eventColumns);

                        // 写入缓存
                        cacheList.put(queryInfo.getEsQueryId(), eventColumns);
                    } else {
                        // 记录ES不存在
                        noCacheList.add(queryInfo.getEsQueryId());
                    }
                }

                // 批量写入缓存
                saveEventCache(noCacheQueryList.get(0).getMappingTable().getTableKey(), cacheList, true);
                // 批量写入无缓存标志
                saveEmpty(noCacheQueryList.get(0).getMappingTable().getTableKey(), noCacheList);
                time.end();
            }
            return keyValues;
        } else {
            return new HashMap<>(0);
        }
    }

    /**
     * 从缓存中查询
     *
     * @param childQueryInfoList 查询列表
     * @param keyValues          结果集
     * @return 未缓存结果
     */
    private List<ChildQueryInfo> findCacheQuery(List<ChildQueryInfo> childQueryInfoList, Map<String, Map<String, String>> keyValues) {
        Time time = new Time("【ES】从Redis批量查询子表数据。");
        // 表名
        String tableKey = childQueryInfoList.get(0).getMappingTable().getTableKey();
        List<String> ids = childQueryInfoList.stream().map(ChildQueryInfo::getEsQueryId).collect(Collectors.toList());
        // 缓存批量查询
        Map<String, Map<String, String>> cacheList = getEventCache(tableKey, ids);
        // 查询是否为空
        Map<String, Boolean> isEmptys = isEmpty(tableKey, ids);

        // 过滤未缓存记录
        List<ChildQueryInfo> noCacheQueryList = childQueryInfoList.stream().filter(query -> {
            Map<String, String> values = cacheList.get(query.getEsQueryId());
            // 缓存、ES均不存在，直接返回为空
            // 缓存不存在，不确定ES是否存在
            if (values != null) {
                // 直接缓存查询到，返回
                keyValues.put(query.getEsQueryId(), values);
                return false;

            } else if (isEmptys.getOrDefault(query.getEsQueryId(), false)) {
                // 缓存、ES均不存在，直接返回为空
                return false;
            } else {
                // 缓存不存在，不确定ES是否存在
                return true;
            }
        }).collect(Collectors.toList());

        if (keyValues.keySet().size() > 0) {
            Log.info("【ES】从Redis批量查询子表数据，命中缓存缓存。table：{}，id：{}", childQueryInfoList.get(0).getMappingTable().getTableKey(), keyValues.keySet());
        }
        time.end();
        return noCacheQueryList;
    }

    /**
     * 保存本地缓存
     *
     * @param eventColumns 列
     */
    protected void saveEventCache(String tableKey, String id, Map<String, String> eventColumns) {
        Map<String, Map<String, String>> map = new HashMap<>(1);
        map.put(id, eventColumns);
        saveEventCache(tableKey, map, false);
    }

    /**
     * 保存副本缓存
     *
     * @param tableKey     tableKey 表名
     * @param eventColumns 列值
     */
    private void saveEventCache(String tableKey, Map<String, Map<String, String>> eventColumns, boolean saveLocal) {
        if (saveLocal) {
            // 写入本地缓存
//            eventColumns.entrySet().forEach(entry -> esEventCache.put(getCopyRedisKey(tableKey, entry.getKey()), entry.getValue()));
        }
        // 写入Redis缓存
        redisDao.batch(getRedisSource(), new ArrayList<>(eventColumns.entrySet()),
                (RedisDao.RedisHandle<Map.Entry<String, List<EventColumn>>>) (connection, param) ->
                        connection.setEx(getCopyRedisKey(tableKey, param.getKey()).getBytes(), 60, JsonUtils.toJson(param.getValue()).getBytes()
                        ));
    }

    /**
     * 查询本地缓存
     *
     * @param tableKey 表名
     * @param ids      主键值
     * @return 事件
     */
    private Map<String, Map<String, String>> getEventCache(String tableKey, List<String> ids) {
        if (ids.size() == 0) {
            return new HashMap<>(0);
        }
        Map<String, Map<String, String>> events = new HashMap<>(ids.size());

//        // 本地缓存查询
//        List<String> l2_ids = ids.stream().filter(id -> {
//            Map<String, String> value = esEventCache.getIfPresent(getCopyRedisKey(tableKey, id));
//            if(value!=null){
//                events.put(id, value);
//                return false;
//            }else {
//                return true;
//            }
//        }).collect(Collectors.toList());

        // Redis缓存查询
        List<String> rs = redisDao.multGet(getRedisSource(), ids.stream().map(id -> getCopyRedisKey(tableKey, id)).collect(Collectors.toList()));
        for (int i = 0; i < ids.size(); i++) {
            events.put(ids.get(i), rs.get(i) != null ? JsonUtils.fromJson(rs.get(i), HashMap.class) : null);
        }
        return events;
    }

    /**
     * 保存空内容
     *
     * @param tableKey 表名
     * @param ids      主键值
     */
    private void saveEmpty(String tableKey, List<String> ids) {
        // 写入本地缓存
//        ids.forEach(id -> emptyEventCache.put(getEmptyRedisKey(tableKey, id), "empty"));

        // 写入Redis缓存
        redisDao.batch(getRedisSource(), ids,
                (RedisDao.RedisHandle<String>) (connection, id) ->
                        connection.setEx(getEmptyRedisKey(tableKey, id).getBytes(), 10, "empty".getBytes())
        );
    }

    /**
     * 读取空内容
     *
     * @param tableKey 表名
     * @param ids      主键值
     * @return 是否为空
     */
    private Map<String, Boolean> isEmpty(String tableKey, List<String> ids) {
        Map<String, Boolean> isEmptys = new HashMap<>(ids.size());

        // 本地缓存查询
//        List<String> l2_ids = ids.stream().filter(id -> {
//            if(emptyEventCache.getIfPresent(getEmptyRedisKey(tableKey, id))!=null){
//                isEmptys.put(id, true);
//                return false;
//            }else {
//                return true;
//            }
//        }).collect(Collectors.toList());

        // Redis缓存查询
        List<String> rs = redisDao.multGet(getRedisSource(), ids.stream().map(id -> getEmptyRedisKey(tableKey, id)).collect(Collectors.toList()));
        for (int i = 0; i < ids.size(); i++) {
            isEmptys.put(ids.get(i), rs.get(i) != null);
        }
        return isEmptys;
    }

    /**
     * Redis默认地址
     */
    private RedisSyncConfig.RedisDataSourceConfig getRedisSource() {
        return new RedisSyncConfig.RedisDataSourceConfig(redisAddr, redisPassword, redisDatabase);
    }

    /**
     * 副本集Redis Key
     *
     * @param tableKey 表名
     * @param id       主键值
     * @return
     */
    private String getCopyRedisKey(String tableKey, String id) {
        return String.format("LA:CP:%s:%s", tableKey, id);
    }

    /**
     * 空值Redis Key
     *
     * @param tableKey
     * @param id
     * @return
     */
    private String getEmptyRedisKey(String tableKey, String id) {
        return String.format("LA:CP_EP:%s:%s", tableKey, id);
    }

}
