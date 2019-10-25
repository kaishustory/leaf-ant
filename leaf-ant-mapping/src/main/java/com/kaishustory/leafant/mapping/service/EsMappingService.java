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

package com.kaishustory.leafant.mapping.service;

import com.kaishustory.leafant.common.model.*;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.common.utils.StringUtils;
import com.kaishustory.leafant.mapping.dao.EsMappingDao;
import com.kaishustory.leafant.mapping.service.interfaces.IMappingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import static com.kaishustory.leafant.common.constants.EventConstants.LOAD_STATUS_NO;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_ES;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_REDIS;

/**
 * ElasticSearch映射管理
 *
 * @author liguoyang
 * @create 2019-08-21 15:40
 **/
@Service
@ConditionalOnProperty(name = "message.mapping.producer", havingValue = "true")
public class EsMappingService implements IMappingService {

    /**
     * ES映射管理
     */
    @Autowired
    private EsMappingDao esMappingDao;

    /**
     * Redis 映射管理
     */
    @Autowired
    private RedisMappingService redisMappingService;

    /**
     * 配置同步管理
     */
    @Autowired
    private MappingSyncService mappingSyncService;

    /**
     * Redis地址
     */
    @Value("${redis.url}")
    private String redisAddr;

    /**
     * Redis密码
     */
    @Value("${redis.password}")
    private String redisPassword;

    /**
     * Redis数据库
     */
    @Value("${redis.database}")
    private int redisDatabase;

    /**
     * Es映射转Es子表独立映射
     *
     * @param child       Es子表映射
     * @param masterIndex 主表索引
     * @param esAddr      ES地址
     * @return ES子表独立映射
     */
    public static EsSyncConfig toChildEsConfig(EsSyncMappingTable child, String masterIndex, String esAddr) {
        EsSyncConfig esSyncConfig = new EsSyncConfig();
        EsSyncMappingTable master = child.copy();

        esSyncConfig.setId(child.getEsCopyMappingId());
        esSyncConfig.setMasterTable(master);
        esSyncConfig.setMult(child.getChildTable() != null && child.getChildTable().size() > 0);
        esSyncConfig.setEsIndexManager(true);
        esSyncConfig.setInit(LOAD_STATUS_NO);
        esSyncConfig.setSync(false);
        esSyncConfig.setShow(false);
        esSyncConfig.setEsAddr(esAddr);

        String index = StringUtils.isNotNull(child.getEsCopyIndex()) ? child.getEsCopyIndex() : String.format("child:%s:%s", masterIndex, child.getTableKey().replaceAll(":", "-"));
        esSyncConfig.setIndex(index);
        esSyncConfig.setType(index);

        master.setMaster(true);
        master.setChildTable(child.getChildTable());
        return esSyncConfig;
    }

    /**
     * 创建ES索引
     *
     * @param esSyncConfig    同步映射
     * @param createEsMapping ES映射创建处理
     */
    public Option<String> createIndex(EsSyncConfig esSyncConfig, CreateEsMapping createEsMapping) {

        // 创建ES索引
        if (esSyncConfig.isEsIndexManager()) {
            boolean index = createEsMapping.handle(esSyncConfig);
            if (!index) {
                Log.error("保存ES索引失败！index：{}", esSyncConfig.getIndex());
                return Option.error("保存ES索引失败！");
            }
        }

        // 多表结构：为子表建立副本，用于子表数据查询
        if (esSyncConfig.isMult()) {
            Option<String> childResult = createCopyChild(esSyncConfig, createEsMapping);
            if (childResult.error()) {
                Log.error("创建子表副本时发送异常。index：", esSyncConfig.getIndex());
                return childResult;
            }
        }

        // 保存同步映射配置
        Option<String> mapping = esMappingDao.saveConfig(esSyncConfig);
        if (!mapping.exist()) {
            Log.error("保存Mongo映射失败！index：{}", esSyncConfig.getIndex());
            return Option.error("保存Mongo映射失败！");
        }

        // 通知同步映射更新
        boolean sync = mappingSyncService.sync(TYPE_ES);
        if (!sync) {
            Log.error("同步索引映射配置失败！index：{}", esSyncConfig.getIndex());
            return Option.error("同步索引映射配置失败！");
        }
        Log.info("创建ES索引成功！index：{}, type：{}", esSyncConfig.getIndex(), esSyncConfig.getType());
        return mapping;
    }

    /**
     * 创建副本子表
     *
     * @param esSyncConfig ES同步配置
     */
    private Option<String> createCopyChild(EsSyncConfig esSyncConfig, CreateEsMapping createEsMapping) {

        // Redis 副本子表
        if (TYPE_REDIS.equals(esSyncConfig.getCopyChildType())) {

            return esSyncConfig.getTableList().stream().filter(EsSyncMappingTable::isChild).map(child -> {
                // ES转为Redis配置
                RedisSyncConfig redisSyncConfig = toChildRedisConfig(child, esSyncConfig.getMasterTable().getSourceTable());
                // 创建Redis副本
                Option<String> redisResult = redisMappingService.createIndex(redisSyncConfig);
                if (redisResult.exist()) {
                    // 记录Redis副本信息
                    child.setRedisMappingId(redisResult.get());
                    child.setRedisKeyPrefix(redisSyncConfig.getRedisKeyPrefix());
                } else {
                    Log.error("为子表创建Redis副本时发生异常！table：{}，errmsg：{}", child.getSourceTable(), redisResult.getErrmsg());
                }
                return redisResult;
            })
                    .reduce((a, b) -> {
                        if (a.error()) {
                            return a;
                        } else if (b.error()) {
                            return b;
                        } else {
                            return a;
                        }
                    })
                    .orElse(Option.empty());

            // ES 副本子表
        } else if (TYPE_ES.equals(esSyncConfig.getCopyChildType())) {

            return esSyncConfig.getMasterTable().getChildTable().stream().map(child -> {
                // ES子表转单独配置
                EsSyncConfig childEsConfig = toChildEsConfig(child, esSyncConfig.getIndex(), esSyncConfig.getEsAddr());
                // 创建子表索引
                Option<String> esResult = this.createIndex(childEsConfig, createEsMapping);
                if (esResult.exist()) {
                    // 记录ES副本信息
                    child.setEsCopyMappingId(esResult.get());
                    child.setEsCopyIndex(childEsConfig.getIndex());
                } else {
                    Log.error("为子表创建ES副本时发生异常！table：{}，errmsg：{}", child.getSourceTable(), esResult.getErrmsg());
                }
                return esResult;
            })
                    .reduce((a, b) -> {
                        if (a.error()) {
                            return a;
                        } else if (b.error()) {
                            return b;
                        } else {
                            return a;
                        }
                    })
                    .orElse(Option.empty());
        }
        return Option.empty();
    }

    /**
     * 更改已初始化状态
     *
     * @param loadStatus 状态
     */
    @Override
    public void updateInitialized(LoadStatus loadStatus) {
        esMappingDao.updateInitialized(loadStatus);
        Log.info("ES 更新初始化状态！mappingId：{}，loadStatus：{}", loadStatus.getMappingId(), loadStatus.getLoadStatus());
    }

    /**
     * 更新同步状态
     *
     * @param syncStatus 同步状态
     * @return 是否成功
     */
    @Override
    public Option updateSyncStatus(SyncStatus syncStatus) {

        // 更新同步状态
        esMappingDao.updateSync(syncStatus);
        // 通知同步映射更新
        if (!mappingSyncService.sync(TYPE_ES)) {
            return Option.error("ES 同步映射配置失败！");
        }
        Log.info("ES 更新同步状态成功！mappingId：{}，syncStatus：{}", syncStatus.getMappingId(), syncStatus.isSync());
        return Option.of("OK");
    }

    /**
     * ES映射转Redis映射
     *
     * @param child       ES子表映射
     * @param masterTable 主表名称
     * @return Redis子表映射
     */
    private RedisSyncConfig toChildRedisConfig(EsSyncMappingTable child, String masterTable) {
        RedisSyncConfig redisSyncConfig = new RedisSyncConfig();
        redisSyncConfig.setSourceRds(child.getSourceRds());
        redisSyncConfig.setSourceDatabase(child.getSourceDatabase());
        redisSyncConfig.setSourceTable(child.getSourceTable());
        redisSyncConfig.setRedisKeyPrefix(String.format("child:%s:%s", masterTable, child.getTableKey().replaceAll(":", "-")));
        redisSyncConfig.setSimplifyField(false);
        redisSyncConfig.setDataSourceConfig(child.getDataSourceConfig());
        redisSyncConfig.setRedisDataSourceConfig(new RedisSyncConfig.RedisDataSourceConfig(redisAddr, redisPassword, redisDatabase));
        redisSyncConfig.setJoinColumn(child.getFieldMapping().stream().filter(EsSyncMappingField::isJoinKey).map(EsSyncMappingField::getSourceColumn).findFirst().orElse(""));
        return redisSyncConfig;
    }

    public interface CreateEsMapping {
        /**
         * ES创建映射处理
         *
         * @param esSyncConfig ES配置
         * @return 是否成功
         */
        boolean handle(EsSyncConfig esSyncConfig);
    }
}
