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

package com.kaishustory.leafant.mapping.cache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 映射配置Cache
 **/
@Component
public class AllMappingCache {

    @Autowired
    private MqMappingCache mqMappingCache;

    @Autowired
    private EsMappingCache esMappingCache;

    @Autowired
    private RedisMappingCache redisMappingCache;

    @Autowired
    private MySQLMappingCache mySQLMappingCache;

    /**
     * 是否存在配置
     *
     * @param rds      实例
     * @param database 数据库
     * @param table    表
     * @return 是否存在配置
     */
    public boolean has(String rds, String database, String table) {
        if (mqMappingCache.getMapping(rds, database, table).exist()) {
            return true;
        }
        if (esMappingCache.getMapping(rds, database, table).exist()) {
            return true;
        }
        if (redisMappingCache.getMapping(rds, database, table).exist()) {
            return true;
        }
        return mySQLMappingCache.getMapping(rds, database, table).exist();
    }
}
