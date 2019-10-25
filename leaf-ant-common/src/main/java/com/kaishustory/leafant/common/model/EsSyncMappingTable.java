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

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * ElasticSearch表同步映射配置
 **/
@Data
public class EsSyncMappingTable {

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private EsSyncConfig config_;

    /**
     * 是否主表
     */
    private boolean isMaster;

    /**
     * 数据源实例
     */
    private String sourceRds;

    /**
     * 数据源库
     */
    private String sourceDatabase;

    /**
     * 数据源表
     */
    private String sourceTable;

    /**
     * 字段映射
     */
    private List<EsSyncMappingField> fieldMapping;

    /**
     * MySQL数据源配置
     */
    private SyncDataSourceConfig dataSourceConfig;

    /**
     * 子表结构映射
     */
    private List<EsSyncMappingTable> childTable;

    /**
     * Redis索引KEY前缀（子表Redis副本）(子表字段)
     */
    private String redisKeyPrefix;

    /**
     * Redis子表副本映射ID (子表字段)
     */
    private String redisMappingId;

    /**
     * ES索引名称（子表ES副本）(子表字段)
     */
    private String esCopyIndex;

    /**
     * ES子表副本映射ID (子表字段)
     */
    private String esCopyMappingId;


    /**
     * 获得表Key
     *
     * @return 缓存KEY
     */
    public String getTableKey() {
        return String.format("%s:%s:%s", sourceRds, sourceDatabase, sourceTable);
    }

    /**
     * 获得Redis key
     *
     * @param id 主键值
     * @return Redis key
     */
    public String getRedisKey(String id) {
        return redisKeyPrefix + ":" + id;
    }

    /**
     * 是否主表（结构树中，根节点就是主表）
     * <p>
     * |      主表
     * /  |  \   子表
     * /   |   \  子表 && 叶子子表
     */
    public boolean isMaster() {
        if (config_ == null) {
            return isMaster;
        } else {
            return !isMult() || isMaster;
        }
    }

    /**
     * 是否子表（结构树中，非根节点，都是子表）
     * <p>
     * |      主表
     * /  |  \   子表
     * /   |   \  子表 && 叶子子表
     */
    public boolean isChild() {
        if (config_ == null) {
            return !isMaster;
        } else {
            return isMult() && !isMaster;
        }
    }

    /**
     * 是否是叶子子表（结构树中，最末一级节点为页子子表）
     * <p>
     * |      主表
     * /  |  \   子表
     * /   |   \  子表 && 叶子子表
     */
    public boolean isLeafChild() {
        if (config_ == null) {
            return (childTable == null || childTable.size() == 0);
        } else {
            return isMult() && (childTable == null || childTable.size() == 0);
        }
    }

    /**
     * 是否是副本子表
     *
     * @return
     */
    public boolean isCopyChild() {
        return this.getIndex().startsWith("child:");
    }

    /**
     * 写入配置信息
     *
     * @param syncConfig 配置
     */
    public void setConfigInfo(EsSyncConfig syncConfig) {
        this.config_ = syncConfig;
    }

    public String getConfigId() {
        return this.config_.getId();
    }

    public boolean isMult() {
        return this.config_.isMult();
    }

    public String getIndex() {
        return this.config_.getIndex();
    }

    public String getType() {
        return this.config_.getType();
    }

    public String getEsAddr() {
        return this.config_.getEsAddr();
    }

    public EsSyncConfig getConfig() {
        return this.config_;
    }

    /**
     * 复制
     */
    public EsSyncMappingTable copy() {
        EsSyncMappingTable copy = new EsSyncMappingTable();
        copy.setRedisMappingId(this.getRedisMappingId());
        copy.setRedisKeyPrefix(this.getRedisKeyPrefix());
        copy.setMaster(this.isMaster);
        copy.setSourceTable(this.getSourceTable());
        copy.setSourceDatabase(this.getSourceDatabase());
        copy.setSourceRds(this.getSourceRds());
        copy.setFieldMapping(this.getFieldMapping());
        copy.setDataSourceConfig(this.getDataSourceConfig());
        copy.setChildTable(this.getChildTable());
        copy.setEsCopyIndex(this.getEsCopyIndex());
        copy.setEsCopyMappingId(this.getEsCopyMappingId());
        return copy;
    }

}
