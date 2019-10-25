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

import com.kaishustory.leafant.common.constants.EventConstants;
import com.kaishustory.leafant.common.model.EsSyncMappingField;
import com.kaishustory.leafant.common.model.EsSyncMappingTable;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.transform.es.dao.ElasticSearchDao;
import com.kaishustory.leafant.transform.es.model.ChildQueryInfo;
import com.kaishustory.leafant.transform.es.model.EsEvent;
import com.kaishustory.leafant.transform.es.model.EsUpdate;
import com.kaishustory.leafant.transform.es.model.EsUpdateQuery;
import com.kaishustory.leafant.transform.redis.service.RedisQueryService;
import io.searchbox.action.AbstractAction;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Update;
import io.searchbox.core.UpdateByQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.MappingConstants.*;

/**
 * ElasticSearch结构转换服务
 **/
@Slf4j
@Service
public class EsTransformService {

    /**
     * ElasticSearch操作
     */
    @Autowired
    private ElasticSearchDao elasticSearchDao;

    /**
     * Redis查询
     */
    @Autowired
    private RedisQueryService redisQueryService;

    /**
     * ES查询
     */
    @Autowired
    private EsQueryService esQueryService;

    /**
     * ElasticSearch同步事件处理（单表事件处理）
     *
     * @param esAddr   ElasticSearch地址
     * @param esEvents 事件列表
     * @param source   来源（canal：数据变更事件，init：数据初始化）
     */
    public void singleEventHandle(String esAddr, List<EsEvent> esEvents, String source) {

        EsEvent esEvent = esEvents.get(0);
        // 批量命令
        elasticSearchDao.bulk(esAddr, esEvent.getMapping().getIndex(), esEvent.getMapping().getType(),
                esEvents.stream().map(event -> {
                    // 事件转换处理
                    switch (event.getEvent().getType()) {

                        /** 新增操作 **/
                        case EventConstants.TYPE_INSERT:
                            return addAll(event, source);

                        /** 修改操作 **/
                        case EventConstants.TYPE_UPDATE:
                            return updateAll(event);

                        /** 删除操作 **/
                        case EventConstants.TYPE_DELETE:
                            return deleteAll(event);

                        default:
                            Log.error("未知事件类型。type：{}", event.getEvent().getType());
                            return null;

                    }
                }).filter(Objects::nonNull).collect(Collectors.toList())
        );
    }

    /**
     * ElasticSearch同步事件处理（多表事件处理）
     *
     * @param esAddr   ElasticSearch地址
     * @param esEvents 事件列表
     * @param source   来源（canal：数据变更事件，init：数据初始化）
     */
    public void multEventHandle(String esAddr, List<EsEvent> esEvents, String source) {

        EsEvent esEvent = esEvents.get(0);

        // 补充子表字段
        extChildField(esEvents);

        List<AbstractAction> actions = esEvents.stream().map(event -> {
            // 事件转换处理
            switch (event.getEvent().getType()) {

                /** 新增操作 **/
                case EventConstants.TYPE_INSERT: {
                    if (event.getMapping().isMaster()) {
                        // ES：新增数据
                        return addAll(event, source);
                    } else {
                        // ES：按外键查询更新，updateByQuery
                        return addChild(event);
                    }
                }
                /** 修改操作 **/
                case EventConstants.TYPE_UPDATE: {
                    if (event.getMapping().isMaster()) {
                        // ES：按ID，更新字段
                        return update(event);
                    } else {
                        // ES：按外键查询更新，updateByQuery
                        return updateChild(event);
                    }
                }
                /** 删除操作 **/
                case EventConstants.TYPE_DELETE: {
                    if (event.getMapping().isMaster()) {
                        // ES：按ID，删除文档
                        return deleteAll(event);
                    } else {
                        // ES：按外键查询更新为空，updateByQuery
                        return deleteChild(event);
                    }
                }
                default:
                    Log.error("未知事件类型。type：{}", event.getEvent().getType());
                    return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());

        // 批量处理命令
        elasticSearchDao.bulk(esAddr, esEvent.getMapping().getIndex(), esEvent.getMapping().getType(), actions.stream().filter(action -> action instanceof BulkableAction).map(action -> (BulkableAction) action).collect(Collectors.toList()));

        // 逐条执行命令
        actions.stream().filter(action -> !(action instanceof BulkableAction)).forEach(action ->
                elasticSearchDao.execr(esAddr, esEvent.getMapping().getIndex(), esEvent.getMapping().getType(), action)
        );
    }

    /**
     * 新增操作
     *
     * @param event  事件
     * @param source 来源（canal：数据变更事件，init：数据初始化）
     * @return 新增处理
     */
    private Index addAll(EsEvent event, String source) {
        // 文档ID
        String id = getId(event);
        // 保存本地缓存
        if (event.getMapping().isCopyChild() && SOURCE_CANAL.equals(source)) {
            esQueryService.saveEventCache(event.getMapping().getTableKey(), id, event.getEvent().getAllColumns().stream().filter(e -> e.getName() != null).collect(Collectors.toMap(EventColumn::getName, e -> e.getValue() != null ? e.getValue() : "")));
        }
        // 更新ES文档
        Log.info("【ES】新增文档 {}, index：{}，type：{}, id：{}，delay：{}，insert：{}", event.getMapping().getTableKey(), event.getMapping().getIndex(), event.getMapping().getType(), id, (System.currentTimeMillis() - event.getEvent().getExecuteTime()) + "/ms", JsonUtils.toJson(event.getEsData()));
        return new Index.Builder(event.getEsData()).index(event.getMapping().getIndex()).type(event.getMapping().getType()).id(id).build();
    }

    /**
     * 更新操作
     *
     * @param event 事件
     * @return 更新处理
     */
    private Index updateAll(EsEvent event) {
        // 文档ID
        String id = getId(event);
        // 更新ES文档
        Log.info("【ES】更新文档 {}, index：{}，type：{}, id：{}，delay：{}，update：{}", event.getMapping().getTableKey(), event.getMapping().getIndex(), event.getMapping().getType(), id, (System.currentTimeMillis() - event.getEvent().getExecuteTime()) + "/ms", JsonUtils.toJson(event.getEsData()));
        return new Index.Builder(event.getEsData()).index(event.getMapping().getIndex()).type(event.getMapping().getType()).id(id).build();
    }

    /**
     * 新增子表操作
     *
     * @param event 事件
     * @return 新增处理
     */
    private UpdateByQuery addChild(EsEvent event) {

        // 更新条件
        Map<String, Object> query = getUpdateQuery(event);
        // 转为Es更新命令
        String updateCmd = new EsUpdateQuery(query, event.getEsData()).toString();
        // 更新ES文档
        Log.info("【ES】新增子文档 {}, index：{}，type：{}, query：{}，delay：{}，insert：{}", event.getMapping().getTableKey(), event.getMapping().getIndex(), event.getMapping().getType(), query, (System.currentTimeMillis() - event.getEvent().getExecuteTime()) + "/ms", JsonUtils.toJson(event.getEsData()));
        return new UpdateByQuery.Builder(updateCmd).addIndex(event.getMapping().getIndex()).addType(event.getMapping().getType()).build();
    }

    /**
     * 更新操作
     *
     * @param event 事件
     * @return 更新处理
     */
    private Update update(EsEvent event) {
        // 提取 MySQL -> ES 字段映射 <MySQL列，ES字段>
        Map<String, String> col2FieldMap = event.getMapping().getFieldMapping().stream().collect(Collectors.toMap(EsSyncMappingField::getSourceColumn, EsSyncMappingField::getField));
        // 更新字段 <ES字段，值>
        Map<String, Object> updateCols = event.getEvent().getAfterColumns().stream().filter(EventColumn::isUpdated).collect(Collectors.toMap(col -> col2FieldMap.getOrDefault(col.getName(), col.getName()), EventColumn::getValue));
        // 转为Es更新命令
        String updateCmd = new EsUpdate(updateCols).toString();

        // 文档ID
        String id = getId(event);
        // 更新ES文档
        Log.info("【ES】修改文档 {}, index：{}，type：{}, id：{}，delay：{}，update：{}", event.getMapping().getTableKey(), event.getMapping().getIndex(), event.getMapping().getType(), id, (System.currentTimeMillis() - event.getEvent().getExecuteTime()) + "/ms", JsonUtils.toJson(updateCols));
        return new Update.Builder(updateCmd).index(event.getMapping().getIndex()).type(event.getMapping().getType()).id(id).build();
    }

    /**
     * 更新子表操作
     *
     * @param event 事件
     * @return 更新处理
     */
    private UpdateByQuery updateChild(EsEvent event) {
        // 更新条件
        Map<String, Object> query = getUpdateQuery(event);
        // 提取 MySQL -> ES 字段映射 <MySQL列，ES字段>
        Map<String, String> col2FieldMap = event.getMapping().getFieldMapping().stream().collect(Collectors.toMap(EsSyncMappingField::getSourceColumn, EsSyncMappingField::getField));
        // 更新字段 <ES字段，值>
        Map<String, Object> updateCols = event.getEvent().getAfterColumns().stream().filter(EventColumn::isUpdated).collect(Collectors.toMap(col -> col2FieldMap.getOrDefault(col.getName(), col.getName()), EventColumn::getValue));
        // 转为Es更新命令
        String updateCmd = new EsUpdateQuery(query, updateCols).toString();
        // 更新ES文档
        Log.info("【ES】修改子文档 {}, index：{}，type：{}, query：{}，delay：{}，update：{}", event.getMapping().getTableKey(), event.getMapping().getIndex(), event.getMapping().getType(), query, (System.currentTimeMillis() - event.getEvent().getExecuteTime()) + "/ms", JsonUtils.toJson(updateCols));
        return new UpdateByQuery.Builder(updateCmd).addIndex(event.getMapping().getIndex()).addType(event.getMapping().getType()).build();
    }

    /**
     * 全部删除操作
     *
     * @param event 事件
     * @return 删除处理
     */
    private Delete deleteAll(EsEvent event) {
        // 文档ID
        String id = getId(event);
        // 删除ES文档
        Log.info("【ES】删除文档 {}, index：{}，type：{}, id：{}，delay：{}", event.getMapping().getTableKey(), event.getMapping().getIndex(), event.getMapping().getType(), id, (System.currentTimeMillis() - event.getEvent().getExecuteTime()) + "/ms");
        return new Delete.Builder(id).index(event.getMapping().getIndex()).type(event.getMapping().getType()).id(id).build();
    }

    /**
     * 删除子表操作
     *
     * @param event 事件
     * @return 删除操作
     */
    private UpdateByQuery deleteChild(EsEvent event) {
        // 更新条件
        Map<String, Object> query = getUpdateQuery(event);
        // 转为Es更新命令
        String updateCmd = new EsUpdateQuery(query, convertDeleteField(event.getMapping())).toString();
        // 更新ES文档
        Log.info("【ES】删除子文档 {}, index：{}，type：{}, query：{}，delay：{}，remove：{}", event.getMapping().getTableKey(), event.getMapping().getIndex(), event.getMapping().getType(), query, (System.currentTimeMillis() - event.getEvent().getExecuteTime()) + "/ms", JsonUtils.toJson(event.getEsData()));
        return new UpdateByQuery.Builder(updateCmd).addIndex(event.getMapping().getIndex()).addType(event.getMapping().getType()).build();
    }

    /**
     * 获得主键
     *
     * @param event 事件
     * @return 主键
     */
    private String getId(EsEvent event) {

        // 副本子表，以关联主表字段作为主键
        if (event.getMapping().isCopyChild()) {
            // 提取关键主表字段作为主键
            Optional<EsSyncMappingField> joinKey = event.getMapping().getFieldMapping().stream().filter(EsSyncMappingField::isJoinKey).findFirst();
            if (joinKey.isPresent()) {
                Object key = event.getEsData().get(joinKey.get().getField());
                if (key != null) {
                    return key.toString();
                } else {
                    return "";
                }
            } else {
                return event.getEvent().getPrimaryKey();
            }

            // 普通表，使用主键
        } else {
            // 主表使用主键
            return event.getEvent().getPrimaryKey();
        }
    }

    /**
     * 获得子表更新条件
     *
     * @param event 事件
     * @return 子表更新条件
     */
    private Map<String, Object> getUpdateQuery(EsEvent event) {
        return event.getMapping().getFieldMapping().stream().filter(EsSyncMappingField::isJoinKey).map(field -> {
            Map<String, Object> q = new HashMap<>(1);
            q.put(field.getJoinMasterEsFieldName(), event.getEsData().get(field.getField()));
            return q;
        }).findFirst().get();
    }

    /**
     * 补充子表字段内容
     *
     * @param esEvents 事件列表
     */
    private void extChildField(List<EsEvent> esEvents) {

        // 按子表层级分组（true：一级子表，false：多级子表）
        List<EsEvent> eventList = esEvents.stream()
                // 仅主表新增操作
                .filter(event -> EventConstants.TYPE_INSERT == event.getEvent().getType() && event.getMapping().isMaster())
                // 过滤无子表事件
                .filter(event -> event.getChildLevel() > 0)
                .collect(Collectors.toList());

        // 按子表层级不同，采用不同处理方式
        if (eventList.size() > 0) {
            eventList.get(0).getMapping().getChildTable().forEach(child -> {
                // 补充子表内容
                extChildField(eventList, child);
            });
        }
    }

    /**
     * 补充子表字段内容（使用Redis批量处理，性能更高）
     *
     * @param esEvents 事件列表
     */
    private void extChildField(List<EsEvent> esEvents, EsSyncMappingTable child) {
        // 主表新增查询
        List<ChildQueryInfo> childQueryInfoList = new ArrayList<>();
        esEvents.forEach(event -> {
            // 补充主表数据
            findChildQueryInfo(childQueryInfoList, event, child);
        });

        // 补充子表字段内容
        if (childQueryInfoList.size() > 0) {
            extEventData(childQueryInfoList);
        }

        if (child.getChildTable().size() > 0) {
            child.getChildTable().forEach(grandson -> extChildField(esEvents, grandson));
        }
    }

    /**
     * 补充子表字段内容
     *
     * @param childQueryInfoList 查询条件列表
     */
    private void extEventData(List<ChildQueryInfo> childQueryInfoList) {

        if (childQueryInfoList.size() > 0) {
            ChildQueryInfo simple = childQueryInfoList.get(0);
            // Redis
            if (TYPE_REDIS.equals(simple.getChildSouce())) {
                // 按RedisMapping分组
                childQueryInfoList.stream().collect(Collectors.groupingBy(ChildQueryInfo::getMappingId)).forEach((redisMappingId, queryList) -> {
                    // 批量查询Redis列信息
                    Map<String, EventColumn[]> childColsCollection = redisQueryService.findKeyValues(redisMappingId, queryList.stream().map(ChildQueryInfo::getRedisKey).collect(Collectors.toList()));
                    // 补充子表字段内容
                    queryList.forEach(queryInfo -> {
                        if (childColsCollection.containsKey(queryInfo.getRedisKey()) && childColsCollection.get(queryInfo.getRedisKey()) != null && childColsCollection.get(queryInfo.getRedisKey()).length > 0) {
                            queryInfo.getEsEvent().addEventData(Arrays.asList(childColsCollection.get(queryInfo.getRedisKey())), queryInfo.getMappingTable());
                        }
                    });
                });

                // ES
            } else if (TYPE_ES.equals(simple.getChildSouce())) {
                // 按EsMapping分组
                childQueryInfoList.stream().collect(Collectors.groupingBy(ChildQueryInfo::getMappingId)).forEach((esMappingId, queryList) -> {
                    // 批量查询ES列信息
                    Map<String, Map<String, String>> childColsCollection = esQueryService.findKeyValues(queryList);
                    // 补充子表字段内容
                    queryList.forEach(queryInfo -> {
                        if (childColsCollection.containsKey(queryInfo.getEsQueryId()) && childColsCollection.get(queryInfo.getEsQueryId()) != null && childColsCollection.get(queryInfo.getEsQueryId()).size() > 0) {
                            // 补充子表字段
                            queryInfo.getEsEvent().addEventData(
                                    childColsCollection.get(queryInfo.getEsQueryId())
                                    , queryInfo.getMappingTable()
                            );
                        }
                    });
                });
            }
        }
    }


    /**
     * 收集子表字段查询信息
     *
     * @param childQueryInfoList 子表查询信息
     * @param event              事件信息
     * @param child              子表定义
     */
    private void findChildQueryInfo(List<ChildQueryInfo> childQueryInfoList, EsEvent event, EsSyncMappingTable child) {
        // 提取所有外连表，字段内容
        child.getFieldMapping().stream()
                // 过滤非外键
                .filter(EsSyncMappingField::isJoinKey)
                // 提取外键字段
                .forEach(field -> {
                    String joinField = field.getJoinMasterEsFieldName();
                    // 获得外键值
                    Object foreignId = event.getEsData().get(joinField);
                    if (foreignId != null) {
                        if (child.getEsCopyMappingId() != null) {
                            // ES：按外键ID查询，子表数据
                            childQueryInfoList.add(new ChildQueryInfo(event, child, child.getEsCopyMappingId(), child.getEsAddr(), child.getEsCopyIndex(), foreignId.toString()));

                        } else if (child.getRedisMappingId() != null) {
                            // Redis：按外键ID查询，子表数据
                            childQueryInfoList.add(new ChildQueryInfo(event, child, child.getRedisMappingId(), child.getRedisKey(foreignId.toString())));
                        }
                    }
                });
    }

    /**
     * 提取所有删除字段（包括子表）
     *
     * @param table 表定义
     * @return 字段列表
     */
    private Map<String, Object> convertDeleteField(EsSyncMappingTable table) {
        Map<String, Object> fields = table.getFieldMapping().stream().map(EsSyncMappingField::getField).collect(Collectors.toMap(f -> f, f -> "delete"));
        table.getChildTable().forEach(child -> fields.putAll(convertDeleteField(child)));
        return fields;
    }

}
