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

/**
 * Redis子表副本查询信息
 **/
@Data
public class ChildRedisQueryInfo {

    /**
     * Es事件信息
     */
    private EsEvent esEvent;

    /**
     * Redis配置ID
     */
    private String redisMappingId;

    /**
     * Redis Key
     */
    private String redisKey;

    /**
     * 映射信息
     */
    private EsSyncMappingTable mappingTable;

    public ChildRedisQueryInfo(EsEvent esEvent, EsSyncMappingTable mappingTable, String redisMappingId, String redisKey) {
        this.esEvent = esEvent;
        this.redisMappingId = redisMappingId;
        this.redisKey = redisKey;
        this.mappingTable = mappingTable;
    }
}
