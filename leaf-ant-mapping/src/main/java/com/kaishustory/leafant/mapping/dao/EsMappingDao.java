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

import com.kaishustory.leafant.common.model.EsSyncConfig;
import com.kaishustory.leafant.common.model.EsSyncMappingTable;
import com.kaishustory.leafant.common.model.LoadStatus;
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
 * ElasticSearch映射配置管理
 *
 * @author liguoyang
 * @create 2019-07-23 11:41
 **/
@Component
public class EsMappingDao {

    /**
     * Mongo集合
     */
    private final String collection = "elasticsearch_mapping";

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
     * 按ID查询
     *
     * @param id ID
     * @return
     */
    public EsSyncConfig findById(String id) {
        return setTableConfigInfo(mongoTemplate.findById(id, EsSyncConfig.class, collection));
    }

    /**
     * 查询全部ES映射结构
     *
     * @return
     */
    public List<EsSyncConfig> findAllMapping() {
        List<EsSyncConfig> esSyncConfigList = mongoTemplate.find(Query.query(Criteria.where("env").is(env)), EsSyncConfig.class, collection);
        // 表写入配置信息
        setTableConfigInfo(esSyncConfigList);
        return esSyncConfigList;
    }

    /**
     * 查询有效ES映射结构
     *
     * @param filterSync 是否过滤未启用配置
     * @return
     */
    public List<EsSyncMappingTable> findSyncMapping(boolean filterSync) {
        List<EsSyncConfig> esSyncConfigModels = mongoTemplate.find(Query.query(Criteria.where("env").is(env)), EsSyncConfig.class, collection);
        // 表写入配置信息
        setTableConfigInfo(esSyncConfigModels);
        return esSyncConfigModels.stream()
                // 过滤未生效配置
                .filter(config -> config.isSync() || !filterSync)
                // 将主子表结构转为列表结构
                .flatMap(config -> config.getTableList().stream())
                .collect(Collectors.toList());
    }

    /**
     * 保存ES映射结构
     *
     * @param esSyncConfig ES映射配置
     */
    public Option<String> saveConfig(EsSyncConfig esSyncConfig) {
        try {
            // 环境
            esSyncConfig.setEnv(env);
            // 创建时间
            esSyncConfig.setCreateTime(new Date());
            // 更新时间
            esSyncConfig.setUpdateTime(new Date());
            // 保存ES映射关系
            mongoTemplate.save(esSyncConfig, collection);
            Log.info("MongoDB 保存ES映射结构成功！{}", esSyncConfig);
            return Option.of(esSyncConfig.getId());
        } catch (Exception e) {
            Log.error("MongoDB 保存ES映射结构异常！{}", esSyncConfig, e);
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

    /**
     * 表配置写入映射信息
     *
     * @param list 配置列表
     */
    private List<EsSyncConfig> setTableConfigInfo(List<EsSyncConfig> list) {
        list.forEach(this::setTableConfigInfo);
        return list;
    }

    /**
     * 表配置写入映射信息
     *
     * @param config 配置
     */
    private EsSyncConfig setTableConfigInfo(EsSyncConfig config) {
        config.getTableList().forEach(table -> {
            // 写入配置信息
            table.setConfigInfo(config);
        });
        return config;
    }

}
