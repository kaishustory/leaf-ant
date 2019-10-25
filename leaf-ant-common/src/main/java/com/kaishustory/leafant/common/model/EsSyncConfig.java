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

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.kaishustory.leafant.common.constants.EventConstants.LOAD_STATUS_NO;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_ES;

/**
 * ElasticSearch同步映射配置
 **/
@Data
public class EsSyncConfig {

    /**
     * 主键
     */
    private String id;

    /**
     * 环境
     */
    private String env;

    /**
     * ES 索引名称
     */
    private String index;

    /**
     * ES 索引类型
     */
    private String type;

    /**
     * 主表结构映射
     */
    private EsSyncMappingTable masterTable;

    /**
     * ES地址列表
     */
    private String esAddr;

    /**
     * 是否管理ES
     */
    private boolean esIndexManager;

    /**
     * 多表结构
     */
    private boolean mult;

    /**
     * 多表结构，副本子表类型（支持：es、redis）
     */
    private String copyChildType = TYPE_ES;

    /**
     * 是否同步
     */
    private boolean sync = false;

    /**
     * 初始化状态：no：未初始化，initing：初始化中，complete：完成，fail：失败
     */
    private String init = LOAD_STATUS_NO;

    /**
     * 是否显示
     */
    private boolean show = true;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;

    /**
     * 表列表
     */
    public List<EsSyncMappingTable> getTableList() {
        List<EsSyncMappingTable> tableList = new ArrayList<>();
        return getTableList(tableList, this.masterTable);
    }

    /**
     * 递归提取表列表
     *
     * @param tableList 表列表
     * @param table     当前表
     * @return 表列表
     */
    private List<EsSyncMappingTable> getTableList(List<EsSyncMappingTable> tableList, EsSyncMappingTable table) {
        tableList.add(table);
        if (table.getChildTable() != null && !table.getChildTable().isEmpty()) {
            table.getChildTable().forEach(child -> getTableList(tableList, child));
        }
        return tableList;
    }

}
