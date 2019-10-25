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

package com.kaishustory.leafant.subscribe.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.StringUtils;
import com.kaishustory.leafant.mapping.cache.AllMappingCache;
import com.kaishustory.leafant.subscribe.common.utils.BeanFactory;
import com.kaishustory.leafant.subscribe.interfaces.ICanalMessageHandle;
import com.kaishustory.leafant.subscribe.model.RowChangeInfo;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.MappingConstants.SOURCE_CANAL;

/**
 * Canal订阅消息处理
 **/
@Slf4j
public class CanalMessageHandle implements ICanalMessageHandle {

    /**
     * 数据库实例
     */
    private String server;

    /**
     * MQ消息发送
     */
    private MqSendService mqSendService = BeanFactory.getBean(MqSendService.class);

    /**
     * 映射配置检查
     */
    private AllMappingCache allMappingCache = BeanFactory.getBean(AllMappingCache.class);


    public CanalMessageHandle(String server) {
        this.server = server;
    }

    /**
     * Canal订阅处理
     *
     * @param message 数据变更消息
     * @return 是否成功
     */
    @Override
    public boolean handle(Message message) {

        //获取事务列表
        List<Event> eventLists = getEventList(message);

        Log.info("实际处理数据条数：{}", eventLists.size());

        // 批量发送MQ
        eventLists.stream()
                // 按【实例:数据库:表】分组
                .collect(Collectors.groupingBy(Event::getTableKey))
                // 分组发送MQ
                .forEach((groupKey, value) -> {
                    // 发送MQ消息
                    mqSendService.send(eventLists);
                });
        return true;
    }

    /**
     * 数据修改操作列表
     *
     * @param message 消息
     * @return 事件列表
     */
    private List<Event> getEventList(Message message) {

        return message.getEntries().stream().filter(entry -> entry.getEntryType() == CanalEntry.EntryType.ROWDATA).map(entry -> {
            try {
                // 事务提取
                /** 数据变更 **/
                //解析数据变更信息
                val rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                //事件类型
                val eventType = rowChange.getEventType();

                //数据修改命令（非查询命令、非表结构修改命令）
                if (eventType != CanalEntry.EventType.QUERY && !rowChange.getIsDdl()) {
                    //记录数据变更
                    return new RowChangeInfo(entry.getHeader(), rowChange);
                } else {
                    return null;
                }

            } catch (InvalidProtocolBufferException e) {
                log.error("ProtocolBuffer 解码异常！", e);
                throw new RuntimeException(e);
            }

        })
                // 过滤无效事务
                .filter(Objects::nonNull)
                // 过滤未配置的事件
                .filter(event -> allMappingCache.has(server, event.getHeader().getSchemaName(), event.getHeader().getTableName()))
                // 将事务拆分为事件列表
                .flatMap(event -> event.getRowChange().getRowDatasList().stream().map(row ->
                        // 转为事件
                        new Event(
                                event.getHeader().getSourceType().name(), // 数据库类型
                                SOURCE_CANAL, // 来源 Canal
                                server, // 数据库实例名称
                                event.getHeader().getSchemaName(), // 数据库
                                event.getHeader().getTableName(), // 表
                                event.getRowChange().getEventType().getNumber(), // 操作类型（1：新增，2：修改，3：删除）
                                event.getRowChange().getEventType().getValueDescriptor().getName(), // 操作类型名称
                                getPrimaryKey(row), // 主键值
                                toColumnList(row.getBeforeColumnsList()), // 之前字段内容
                                toColumnList(row.getAfterColumnsList()), // 之后字段内容
                                event.getHeader().getExecuteTime(), // 发生时间
                                event.getHeader().getServerId(),
                                event.getHeader().getLogfileName(),
                                event.getHeader().getLogfileOffset()
                        )))
                .collect(Collectors.toList());
    }

    /**
     * 主键值
     *
     * @param row 字段信息
     * @return 主键值
     */
    private String getPrimaryKey(CanalEntry.RowData row) {
        return (row.getBeforeColumnsCount() > 0 ? row.getBeforeColumnsList() : row.getAfterColumnsList()).stream().filter(CanalEntry.Column::getIsKey).map(CanalEntry.Column::getValue).reduce((a, b) -> a + ":" + b).orElse("");
    }

    /**
     * 列字段转换
     *
     * @param columns
     * @return
     */
    private List<EventColumn> toColumnList(List<CanalEntry.Column> columns) {
        return columns.stream().map(column -> new EventColumn(column.getIsKey(), column.getIndex(), column.getName(), StringUtils.isNotNull(column.getValue()) ? column.getValue() : null, column.getMysqlType(), column.getSqlType(), column.getUpdated(), column.getIsNull())).collect(Collectors.toList());
    }
}
