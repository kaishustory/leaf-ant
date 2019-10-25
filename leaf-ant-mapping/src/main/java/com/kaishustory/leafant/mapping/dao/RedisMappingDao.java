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

package com.kaishustory.leafant.mapping.dao;

import com.kaishustory.leafant.common.model.LoadStatus;
import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.model.SyncStatus;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis映射配置管理
 **/
@Component
public class RedisMappingDao {

    /**
     * Mongo集合
     */
    private final String collection = "redis_mapping";

    /**
     * MongoDB
     */
    @Resource(name = "mappingMongoTemplate")
    private MongoTemplate mongoTemplate;

    /**
     * 环境
     */
    @Value("${spring.profiles.active}")
    private String env;

    /**
     * 查询全部Redis映射结构
     */
    public List<RedisSyncConfig> findAllMapping() {
        return mongoTemplate.find(Query.query(Criteria.where("env").is(env)), RedisSyncConfig.class, collection);
    }

    /**
     * 查询有效Redis映射结构
     *
     * @param filterSync 是否过滤未启用配置
     * @return
     */
    public List<RedisSyncConfig> findSyncMapping(boolean filterSync) {
        List<RedisSyncConfig> redisSyncConfigModels = mongoTemplate.find(Query.query(Criteria.where("env").is(env)), RedisSyncConfig.class, collection);
        return redisSyncConfigModels.stream()
                // 过滤未生效配置
                .filter(config -> config.isSync() || !filterSync)
                .collect(Collectors.toList());
    }

    /**
     * 查询Redis映射
     *
     * @param id 映射ID
     * @return Redis映射
     */
    public RedisSyncConfig findById(String id) {
        return mongoTemplate.findById(id, RedisSyncConfig.class, collection);
    }

    /**
     * 保存Redis映射结构
     *
     * @param redisSyncConfig Redis映射配置
     */
    public Option<String> saveConfig(RedisSyncConfig redisSyncConfig) {
        try {
            // 环境
            redisSyncConfig.setEnv(env);
            // 创建时间
            redisSyncConfig.setCreateTime(new Date());
            // 更新时间
            redisSyncConfig.setUpdateTime(new Date());
            // 保存Redis映射关系
            mongoTemplate.save(redisSyncConfig, collection);
            Log.info("MongoDB 保存Redis映射结构成功！{}", redisSyncConfig);
            return Option.of(redisSyncConfig.getId());
        } catch (Exception e) {
            Log.error("MongoDB 保存Redis映射结构异常！{}", redisSyncConfig, e);
            return Option.error(e.getMessage());
        }

    }

    /**
     * 更改已初始化状态
     *
     * @param loadStatus 状态
     */
    public void updateInitialized(LoadStatus loadStatus) {
        mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(loadStatus.getMappingId())), Update.update("init", loadStatus.getLoadStatus()), collection);
    }

    /**
     * 更改同步状态
     *
     * @param syncStatus 状态
     */
    public void updateSync(SyncStatus syncStatus) {
        mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(syncStatus.getMappingId())), Update.update("sync", syncStatus.isSync()), collection);
    }


}
