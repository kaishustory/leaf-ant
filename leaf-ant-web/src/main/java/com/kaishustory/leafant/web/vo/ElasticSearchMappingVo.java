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

package com.kaishustory.leafant.web.vo;

import lombok.Data;

import java.util.List;

/**
 * ElasticSearch映射Vo
 *
 * @author liguoyang
 * @create 2019-08-02 13:59
 **/
@Data
public class ElasticSearchMappingVo {

    public ElasticSearchMappingVo() {
    }

    public ElasticSearchMappingVo(String mappingId, String elasticSearchIndex, List<ElasticSearchMappingSourceVo> sourceList, boolean sync, String init) {
        this.mappingId = mappingId;
        this.elasticSearchIndex = elasticSearchIndex;
        this.sourceList = sourceList;
        this.sync = sync;
        this.init = init;
    }

    /**
     * 映射ID
     */
    private String mappingId;

    /**
     * 索引
     */
    private String elasticSearchIndex;

    /**
     * 是否同步
     */
    private boolean sync;

    /**
     * 初始化状态
     */
    private String init;

    /**
     * 数据源
     */
    private List<ElasticSearchMappingSourceVo> sourceList;

    @Data
    public static class ElasticSearchMappingSourceVo{

        public ElasticSearchMappingSourceVo() {
        }

        public ElasticSearchMappingSourceVo(boolean master, String sourceDatabase, String sourceTables) {
            this.master = master;
            this.sourceDatabase = sourceDatabase;
            this.sourceTables = sourceTables;
        }

        /**
         * 是否主表
         */
        private boolean master;

        /**
         * 数据库
         */
        private String sourceDatabase;

        /**
         * 数据表
         */
        private String sourceTables;
    }
}
