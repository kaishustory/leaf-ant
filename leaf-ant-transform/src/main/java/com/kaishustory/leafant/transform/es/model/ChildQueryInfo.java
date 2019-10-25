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

import com.kaishustory.leafant.common.model.EsSyncMappingTable;
import lombok.Data;

import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_ES;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_REDIS;

/**
 * 子表副本查询信息
 **/
@Data
public class ChildQueryInfo {

    /**
     * 子表数据源（支持：redis、es）
     */
    private String childSouce;

    /**
     * Es事件信息
     */
    private EsEvent esEvent;

    /**
     * 配置ID
     */
    private String mappingId;

    /**
     * 映射信息
     */
    private EsSyncMappingTable mappingTable;

    /**
     * Redis Key
     */
    private String redisKey;

    /**
     * ES地址
     */
    private String esAddr;

    /**
     * ES 子表副本索引
     */
    private String esIndex;

    /**
     * ES 查询内容
     */
    private String esQueryId;

    public ChildQueryInfo(EsEvent esEvent, EsSyncMappingTable mappingTable, String mappingId, String redisKey) {
        this.childSouce = TYPE_REDIS;
        this.esEvent = esEvent;
        this.mappingId = mappingId;
        this.redisKey = redisKey;
        this.mappingTable = mappingTable;
    }

    public ChildQueryInfo(EsEvent esEvent, EsSyncMappingTable mappingTable, String mappingId, String esAddr, String esIndex, String esQueryId) {
        this.childSouce = TYPE_ES;
        this.esEvent = esEvent;
        this.mappingId = mappingId;
        this.mappingTable = mappingTable;
        this.esAddr = esAddr;
        this.esIndex = esIndex;
        this.esQueryId = esQueryId;
    }
}
