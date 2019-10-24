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

package com.kaishustory.leafant.transform.route;

import com.kaishustory.leafant.common.model.*;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.mapping.cache.EsMappingCache;
import com.kaishustory.leafant.mapping.cache.MqMappingCache;
import com.kaishustory.leafant.mapping.cache.MySQLMappingCache;
import com.kaishustory.leafant.mapping.cache.RedisMappingCache;
import com.kaishustory.leafant.transform.es.model.EsEvent;
import com.kaishustory.leafant.transform.es.service.EsTransformService;
import com.kaishustory.leafant.transform.mq.service.MqTransformService;
import com.kaishustory.leafant.transform.mysql.model.MySQLEvent;
import com.kaishustory.leafant.transform.mysql.service.MySQLTransformService;
import com.kaishustory.leafant.transform.redis.model.RedisEvent;
import com.kaishustory.leafant.transform.redis.service.RedisTransformService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.MappingConstants.*;

/**
 * 事件路由
 *
 * @author liguoyang
 * @create 2019-07-23 10:57
 **/
@Component
public class EventRouteService {

    /**
     * ES同步映射配置管理
     */
    @Autowired
    private EsMappingCache esMappingCache;

    /**
     * Redis同步映射配置管理
     */
    @Autowired
    private RedisMappingCache redisMappingCache;

    /**
     * MQ同步转换配置管理
     */
    @Autowired
    private MqMappingCache mqMappingCache;

    /**
     * MySQL同步映射配置管理
     */
    @Autowired
    private MySQLMappingCache mysqlMappingCache;

    /**
     * ES同步转换处理
     */
    @Autowired
    private EsTransformService esTransformService;

    /**
     * Redis同步转换处理
     */
    @Autowired
    private RedisTransformService redisTransformService;

    /**
     * MQ同步转换处理
     */
    @Autowired
    private MqTransformService mqTransformService;

    /**
     * MySQL同步转换处理
     */
    @Autowired
    private MySQLTransformService mysqlTransformService;


    /**
     * 事件路由转发
     *
     * @param allEventList 事件列表
     */
    public void route(Event... allEventList) {

        // 按 数据库实例+数据库+表名+来源，分组处理
        Arrays.stream(allEventList).collect(Collectors.groupingBy(e -> String.format("%s_%s_%s_%s", e.getServer(), e.getDatabase(), e.getTable(), e.getSource()))).values()
                .forEach(eventList -> {
                    if (eventList != null && eventList.size() > 0) {
                        Event e = eventList.get(0);

                        /** Redis数据转换处理 **/
                        if (SOURCE_CANAL.equals(e.getSource()) || (SOURCE_INIT.equals(e.getSource()) && TYPE_REDIS.equals(e.getTarget()))) {
                            // 查询事件是否有同步映射配置
                            Option<List<RedisSyncConfig>> redisMappingList = redisMappingCache.getMapping(e.getServer(), e.getDatabase(), e.getTable());
                            if (redisMappingList.exist()) {
                                redisMappingList.get().stream()
                                        // 初始化事件，只执行指定映射
                                        .filter(mapping -> (!SOURCE_INIT.equals(e.getSource()) || mapping.getId().equals(e.getMappingId())))
                                        .forEach(mapping -> {
                                            // Redis事件处理
                                            redisTransformService.eventHandle(mapping.getRedisDataSourceConfig(), eventList.stream().map(event -> new RedisEvent(event, mapping)).collect(Collectors.toList()));
                                        });
                            }
                        }

                        /** ElasticSearch数据转换处理 **/
                        if (SOURCE_CANAL.equals(e.getSource()) || (SOURCE_INIT.equals(e.getSource()) && TYPE_ES.equals(e.getTarget()))) {
                            // 查询事件是否有同步映射配置
                            Option<List<EsSyncMappingTable>> esMappingList = SOURCE_INIT.equals(e.getSource()) ?
                                    esMappingCache.getMapping(e.getMappingId(), e.getTable()) :
                                    esMappingCache.getMapping(e.getServer(), e.getDatabase(), e.getTable());
                            if (esMappingList.exist()) {
                                esMappingList.get().forEach(mapping -> {
                                    if (mapping.isMult()) {
                                        // ES多表事件处理
                                        esTransformService.multEventHandle(mapping.getEsAddr(), eventList.stream().map(event -> new EsEvent(event, mapping)).collect(Collectors.toList()), e.getSource());
                                    } else {
                                        // ES单表事件处理
                                        esTransformService.singleEventHandle(mapping.getEsAddr(), eventList.stream().map(event -> new EsEvent(event, mapping)).collect(Collectors.toList()), e.getSource());
                                    }
                                });
                            }
                        }

                        /** MySQL数据转换处理 **/
                        if (SOURCE_CANAL.equals(e.getSource()) || (SOURCE_INIT.equals(e.getSource()) && TYPE_MYSQL.equals(e.getTarget()))) {
                            // 查询事件是否有同步映射配置
                            Option<List<MySQLSyncConfig>> mysqlMappingList = mysqlMappingCache.getMapping(e.getServer(), e.getDatabase(), e.getTable());
                            if (mysqlMappingList.exist()) {
                                mysqlMappingList.get().stream()
                                        // 初始化事件，只执行指定映射
                                        .filter(mapping -> (!SOURCE_INIT.equals(e.getSource()) || mapping.getId().equals(e.getMappingId())))
                                        .forEach(mapping -> {
                                            // MySQL事件处理
                                            mysqlTransformService.eventHandle(eventList.stream().map(event -> new MySQLEvent(event, mapping)).collect(Collectors.toList()));
                                        });
                            }
                        }

                        /** MQ数据转换处理 **/
                        if (SOURCE_CANAL.equals(e.getSource()) || (SOURCE_INIT.equals(e.getSource()) && TYPE_MQ.equals(e.getTarget()))) {
                            Option<List<MqSyncConfig>> mqMappingList = mqMappingCache.getMapping(e.getServer(), e.getDatabase(), e.getTable());
                            if (mqMappingList.exist()) {
                                mqMappingList.get().stream()
                                        // 初始化事件，只执行指定映射
                                        .filter(mapping -> (!SOURCE_INIT.equals(e.getSource()) || mapping.getId().equals(e.getMappingId())))
                                        .forEach(mapping -> {
                                            // MQ事件处理
                                            mqTransformService.eventHandle(mapping, eventList);
                                        });

                            }
                        }
                    }
                });

    }

}
