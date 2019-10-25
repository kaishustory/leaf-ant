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

package com.kaishustory.leafant.transform.es.model;

import com.kaishustory.leafant.common.model.EsSyncMappingField;
import com.kaishustory.leafant.common.model.EsSyncMappingTable;
import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.utils.StringUtils;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.EventConstants.TYPE_DELETE;

/**
 * ElasticSearch同步事件
 **/
@Data
public class EsEvent {

    /**
     * MySQL事件信息
     */
    private Event event;
    /**
     * Es映射结构
     */
    private EsSyncMappingTable mapping;
    /**
     * Es字段内容 <ES字段，值>
     */
    private Map<String, Object> esData = new HashMap<>();
    /**
     * MySQL字段内容 <MySQL列，值>
     */
    private Map<String, Object> mysqlData = new HashMap<>();

    /**
     * 同步事件
     *
     * @param event   事件信息
     * @param mapping 映射关系
     */
    public EsEvent(Event event, EsSyncMappingTable mapping) {
        this.event = event;
        this.mapping = mapping;

        addEventData(event.getType() == TYPE_DELETE ? event.getBeforeColumns() : event.getAfterColumns(), mapping);
    }

    /**
     * 增加数据
     *
     * @param columnList   列值列表
     * @param mappingTable 映射配置
     */
    public void addEventData(List<EventColumn> columnList, EsSyncMappingTable mappingTable) {
        // 提取数据库值 <MySQL列，值>
        mysqlData.putAll(columnList.stream().collect(Collectors.toMap(EventColumn::getName, e -> e.getValue() != null ? e.getValue() : "")));
        // 转换为ES字段值 <ES字段，值>
        mappingTable.getFieldMapping().stream().filter(EsSyncMappingField::isSync).forEach(field -> {
            Object value = mysqlData.get(field.getSourceColumn());
            esData.put(field.getField(), convertEsValue(value));
        });
    }

    /**
     * 增加数据
     *
     * @param columnList   列值列表
     * @param mappingTable 映射配置
     */
    public void addEventData(Map<String, String> columnList, EsSyncMappingTable mappingTable) {
        // 提取数据库值 <MySQL列，值>
        mysqlData.putAll(columnList);
        // 转换为ES字段值 <ES字段，值>
        mappingTable.getFieldMapping().stream().filter(EsSyncMappingField::isSync).forEach(field -> {
            Object value = mysqlData.get(field.getSourceColumn());
            esData.put(field.getField(), convertEsValue(value));
        });
    }

    /**
     * 子表层级数量
     */
    public int getChildLevel() {
        return findChildLevel(0, mapping);
    }

    private int findChildLevel(int level, EsSyncMappingTable mapping) {
        if (mapping.getChildTable().size() > 0) {
            return mapping.getChildTable().stream().map(child -> findChildLevel(level, child)).reduce(Math::max).orElse(level) + 1;
        } else {
            return level;
        }
    }

    /**
     * 转换Es字段内容
     *
     * @param value 内容
     * @return Es内容
     */
    private Object convertEsValue(Object value) {
        if (value != null) {
            if (value instanceof String) {
                if (StringUtils.isNotNull(value.toString())) {
                    // ES 限制字符串最大长度
                    if (((String) value).getBytes().length >= 32766) {
                        return value.toString().substring(0, 32766 / 4);
                    } else {
                        return value;
                    }
                } else {
                    return null;
                }
            } else {
                return value;
            }
        } else {
            return null;
        }

    }

}
