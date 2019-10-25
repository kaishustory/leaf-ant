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

package com.kaishustory.leafant.common.model;

import com.kaishustory.leafant.common.utils.StringUtils;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.EventConstants.*;
import static com.kaishustory.leafant.common.constants.MappingConstants.SOURCE_CANAL;

/**
 * 数据修改事件
 *
 * @author liguoyang
 * @create 2019-07-10 15:09
 **/
@Data
public class Event {

    /**
     * 数据源类型
     */
    private String sourceType;
    /**
     * 数据库实例
     */
    private String server;
    /**
     * 数据库
     */
    private String database;
    /**
     * 表名
     */
    private String table;
    /**
     * 来源（init：初始化加载，canal：MySQL变更事件）
     */
    private String source = SOURCE_CANAL;
    /**
     * 同步目标（ElasticSearch：es，Redis：redis，MQ：mq）（仅source = init使用）
     */
    private String target;
    /**
     * 配置映射ID（仅source = init使用）
     */
    private String mappingId;
    /**
     * 操作类型（1：新增，2：修改，3：删除）
     */
    private int type;
    /**
     * 操作类型名称
     */
    private String typeName;
    /**
     * 主键值
     */
    private String primaryKey;
    /**
     * 修改前列信息
     */
    private List<EventColumn> beforeColumns;
    /**
     * 修改后列信息
     */
    private List<EventColumn> afterColumns;
    /**
     * 时间发生时间
     */
    private long executeTime;
    /**
     * 服务ID
     */
    private long serverId;
    /**
     * MySQL binlog文件名
     */
    private String logfileName;
    /**
     * MySQL binlog位置
     */
    private long logfileOffset;

    public Event() {
    }

    /**
     * 初始化事件使用
     *
     * @param sourceType
     * @param source
     * @param target
     * @param mappingId
     * @param server
     * @param database
     * @param table
     * @param type
     * @param typeName
     * @param primaryKey
     * @param beforeColumns
     * @param afterColumns
     * @param executeTime
     */
    public Event(String sourceType, String source, String target, String mappingId, String server, String database, String table, int type, String typeName, String primaryKey, List<EventColumn> beforeColumns, List<EventColumn> afterColumns, long executeTime) {
        this.sourceType = sourceType;
        this.source = source;
        this.target = target;
        this.mappingId = mappingId;
        this.server = server;
        this.database = database;
        this.table = table;
        this.type = type;
        this.typeName = typeName;
        this.primaryKey = primaryKey;
        this.beforeColumns = beforeColumns;
        this.afterColumns = afterColumns;
        this.executeTime = executeTime;
    }

    /**
     * Canal事件使用
     *
     * @param sourceType
     * @param server
     * @param database
     * @param table
     * @param type
     * @param typeName
     * @param primaryKey
     * @param beforeColumns
     * @param afterColumns
     * @param executeTime
     * @param serverId
     * @param logfileName
     * @param logfileOffset
     */
    public Event(String sourceType, String source, String server, String database, String table, int type, String typeName, String primaryKey, List<EventColumn> beforeColumns, List<EventColumn> afterColumns, long executeTime, long serverId, String logfileName, long logfileOffset) {
        this.sourceType = sourceType;
        this.server = server;
        this.database = database;
        this.table = table;
        this.type = type;
        this.typeName = typeName;
        this.primaryKey = primaryKey;
        this.beforeColumns = beforeColumns;
        this.afterColumns = afterColumns;
        this.executeTime = executeTime;
        this.serverId = serverId;
        this.logfileName = logfileName;
        this.logfileOffset = logfileOffset;
    }

    public int getHashCode() {
        return Objects.hashCode(this);
    }

    /**
     * 获得完整表名
     *
     * @return
     */
    public String getTableKey() {
        return String.format("%s:%s:%s", this.getServer(), this.getDatabase(), this.getTable());
    }

    /**
     * 读取更新字段
     */
    public List<EventColumn> getUpdateColumns() {
        if (type == TYPE_INSERT || type == TYPE_UPDATE) {
            return this.getAfterColumns().stream().filter(EventColumn::isUpdated).collect(Collectors.toList());
        } else if (type == TYPE_DELETE) {
            return this.getBeforeColumns().stream().filter(EventColumn::isKey).collect(Collectors.toList());
        } else {
            return new ArrayList<>(0);
        }
    }

    /**
     * 提取全部字段
     */
    public List<EventColumn> getAllColumns() {
        if (type == TYPE_INSERT || type == TYPE_UPDATE) {
            return this.getAfterColumns();
        } else if (type == TYPE_DELETE) {
            return this.getBeforeColumns();
        } else {
            return new ArrayList<>(0);
        }
    }

    /**
     * 更新字段基本信息 <字段名称, 字段内容>
     */
    public Map<String, String> getUpdateColumnsBase() {
        return getUpdateColumns().stream().collect(Collectors.toMap(EventColumn::getName, e -> StringUtils.isNotNull(e.getValue()) ? e.getValue() : ""));
    }
}
