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
import com.kaishustory.leafant.common.model.MqSyncConfig;
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
 * MQ映射配置管理
 **/
@Component
public class MqMappingDao {

    /**
     * Mongo集合
     */
    private final String collection = "mq_mapping";

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
     * 查询全部MQ映射结构
     *
     * @return
     */
    public List<MqSyncConfig> findAllMapping() {
        return mongoTemplate.find(Query.query(Criteria.where("env").is(env)), MqSyncConfig.class, collection);
    }

    /**
     * 查询有效MQ映射结构
     *
     * @param filterSync 是否过滤未启用配置
     * @return
     */
    public List<MqSyncConfig> findSyncMapping(boolean filterSync) {
        List<MqSyncConfig> mqSyncConfigs = mongoTemplate.find(Query.query(Criteria.where("env").is(env)), MqSyncConfig.class, collection);
        return mqSyncConfigs.stream()
                // 过滤未生效配置
                .filter(config -> config.isSync() || !filterSync)
                .collect(Collectors.toList());
    }

    /**
     * 保存MQ映射结构
     *
     * @param mqSyncConfig MQ映射配置
     */
    public Option<String> saveConfig(MqSyncConfig mqSyncConfig) {
        try {
            // 环境
            mqSyncConfig.setEnv(env);
            // 创建时间
            mqSyncConfig.setCreateTime(new Date());
            // 更新时间
            mqSyncConfig.setUpdateTime(new Date());
            // 保存MQ映射关系
            mongoTemplate.save(mqSyncConfig, collection);
            Log.info("MongoDB 保存MQ映射结构成功！{}", mqSyncConfig);
            return Option.of(mqSyncConfig.getId());
        } catch (Exception e) {
            Log.error("MongoDB 保存MQ映射结构异常！{}", mqSyncConfig, e);
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
