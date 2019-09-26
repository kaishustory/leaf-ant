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

import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_ES;

/**
 * ElasticSearch 初始化信息
 * @author liguoyang
 * @create 2019-08-31 11:24
 **/
@Data
public class EsInitLoadInfo extends InitLoadInfo{

    /**
     * ES映射配置
     */
    private EsSyncConfig esSyncConfig;

    public EsInitLoadInfo(EsSyncConfig esSyncConfig) {
        super(TYPE_ES, esSyncConfig.getId(), esSyncConfig.getMasterTable().getDataSourceConfig());
        this.esSyncConfig = esSyncConfig;
    }
}
