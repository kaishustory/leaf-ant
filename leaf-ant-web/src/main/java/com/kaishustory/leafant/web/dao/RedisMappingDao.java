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

package com.kaishustory.leafant.web.dao;

import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.utils.Page;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Redis映射Dao
 *
 * @author liguoyang
 * @create 2019-08-02 13:49
 **/
@Component
public class RedisMappingDao {

    /**
     * 集合
     */
    private final String collection = "redis_mapping";
    @Resource(name = "mongoTemplate")
    private MongoTemplate mongoTemplate;
    /**
     * 环境
     */
    @Value("${spring.profiles.active}")
    private String env;

    /**
     * 查询全部映射
     *
     * @param page     页号
     * @param pageSize 每页条数
     * @return 映射列表
     */
    public Page<RedisSyncConfig> search(String sourceTable, int page, int pageSize) {
        Criteria criteria = new Criteria();
        Query query = new Query(criteria);
        criteria.and("env").is(env);
        query.skip((page - 1) * pageSize).limit(pageSize);
        query.with(Sort.by(Sort.Direction.DESC, "createTime"));
        return Page.of(mongoTemplate.find(query, RedisSyncConfig.class, collection), searchCount(), page, pageSize);
    }

    /**
     * 查询全部映射数量
     *
     * @return 数量
     */
    private int searchCount() {
        Criteria criteria = new Criteria();
        Query query = new Query(criteria);
        criteria.and("env").is(env);
        return (int) mongoTemplate.count(query, RedisSyncConfig.class, collection);
    }

    /**
     * 查询映射配置
     *
     * @param mappingId 映射配置ID
     * @return 映射配置
     */
    public RedisSyncConfig find(String mappingId) {
        return mongoTemplate.findById(mappingId, RedisSyncConfig.class, collection);
    }
}
