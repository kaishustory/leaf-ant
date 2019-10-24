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

package com.kaishustory.leafant.transform.mapping.listener;

import com.kaishustory.leafant.common.model.*;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.mapping.service.EsMappingService;
import com.kaishustory.leafant.mapping.service.MqMappingService;
import com.kaishustory.leafant.mapping.service.MySQLMappingService;
import com.kaishustory.leafant.mapping.service.RedisMappingService;
import com.kaishustory.leafant.transform.es.dao.ElasticSearchDao;
import com.kaishustory.leafant.transform.es.model.EsMapping;
import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponse;
import com.kaishustory.message.consumer.NettyConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static com.kaishustory.leafant.common.constants.MappingConstants.*;

/**
 * 映射管理监听
 *
 * @author liguoyang
 * @create 2019-09-10 18:17
 **/
@Component
public class MappingManagerListener {

    /**
     * ES映射管理
     */
    @Autowired
    private EsMappingService esMappingService;

    /**
     * ES操作
     */
    @Autowired
    private ElasticSearchDao elasticSearchDao;

    /**
     * Redis映射管理
     */
    @Autowired
    private RedisMappingService redisMappingService;

    /**
     * MySQL映射管理
     */
    @Autowired
    private MySQLMappingService mysqlMappingService;

    /**
     * MQ映射管理
     */
    @Autowired
    private MqMappingService mqMappingService;

    /**
     * Zookeeper地址
     */
    @Value("${zookeeper.url}")
    private String zookeeper;

    /**
     * 同步消息Group
     */
    @Value("${message.group}")
    private String messageGroup;

    /**
     * 映射创建管理Topic
     */
    @Value("${mapping.create.topic}")
    private String mappingCreateType;

    /**
     * 映射同步状态Topic
     */
    @Value("${mapping.sync.topic}")
    private String mappingSyncType;

    /**
     * 创建映射事件监听
     */
    @PostConstruct
    public void createMappingLister() {

        // 监听创建映射消息
        new NettyConsumer(messageGroup, mappingCreateType, zookeeper, rpcRequest -> {
            try {
                // 创建映射
                Option<String> result = createMapping(rpcRequest);
                if (result.exist()) {
                    return new RpcResponse("create-callback", "ok", RpcResponse.STATUS_SUCCESS);
                } else {
                    return new RpcResponse("create-callback", "fail", RpcResponse.STATUS_FAIL);
                }
            } catch (Throwable e) {
                return new RpcResponse("create-callback", "fail", RpcResponse.STATUS_FAIL);
            }
        });

        // 监听同步状态消息
        new NettyConsumer(messageGroup, mappingSyncType, zookeeper, rpcRequest -> {
            SyncStatus syncStatus = JsonUtils.fromJson(rpcRequest.getData(), SyncStatus.class);
            try {
                // 同步状态更新
                Option result = updateSync(rpcRequest.getAction(), syncStatus);
                if (result.exist()) {
                    Log.info("{} 同步状态变更成功。status：{}", rpcRequest.getAction(), syncStatus);
                    return new RpcResponse("sync-callback", "ok", RpcResponse.STATUS_SUCCESS);
                } else {
                    Log.error("{} 同步状态变更失败。status：{}，err：{}", rpcRequest.getAction(), syncStatus, result.getErrmsg());
                    return new RpcResponse("sync-callback", "fail", RpcResponse.STATUS_FAIL);
                }
            } catch (Exception e) {
                Log.error("{} 同步状态变更失败。status：{}", rpcRequest.getAction(), syncStatus, e);
                return new RpcResponse("sync-callback", "fail", RpcResponse.STATUS_FAIL);
            }
        });
    }

    /**
     * 创建映射
     *
     * @param rpcRequest 请求
     * @return 是否成功
     */
    private Option<String> createMapping(RpcRequest rpcRequest) {

        // ES
        if (TYPE_ES.equals(rpcRequest.getAction())) {
            // 创建ES索引
            return esMappingService.createIndex(JsonUtils.fromJson(rpcRequest.getData(), EsSyncConfig.class), (esSyncConfig) -> {
                // 创建ES映射处理
                return elasticSearchDao.createIndex(esSyncConfig.getEsAddr(), esSyncConfig.getIndex(), esSyncConfig.getType(), toEsMapping(esSyncConfig));
            });

            // Redis
        } else if (TYPE_REDIS.equals(rpcRequest.getAction())) {
            return redisMappingService.createIndex(JsonUtils.fromJson(rpcRequest.getData(), RedisSyncConfig.class));

            // MySQL
        } else if (TYPE_MYSQL.equals(rpcRequest.getAction())) {
            return mysqlMappingService.createIndex(JsonUtils.fromJson(rpcRequest.getData(), MySQLSyncConfig.class));

            // MQ
        } else if (TYPE_MQ.equals(rpcRequest.getAction())) {
            return mqMappingService.createIndex(JsonUtils.fromJson(rpcRequest.getData(), MqSyncConfig.class));

        } else {
            Log.errorThrow("无法创建映射，未知映射类型。type：{}", rpcRequest.getAction());
            return null;
        }

    }

    /**
     * 更新同步状态
     *
     * @param type       类型
     * @param syncStatus 状态信息
     * @return 是否成功
     */
    private Option updateSync(String type, SyncStatus syncStatus) {

        // ES
        if (TYPE_ES.equals(type)) {
            return esMappingService.updateSyncStatus(syncStatus);

            // Redis
        } else if (TYPE_REDIS.equals(type)) {
            return redisMappingService.updateSyncStatus(syncStatus);

            // MySQL
        } else if (TYPE_MYSQL.equals(type)) {
            return mysqlMappingService.updateSyncStatus(syncStatus);

            // MQ
        } else if (TYPE_MQ.equals(type)) {
            return mqMappingService.updateSyncStatus(syncStatus);

        } else {
            Log.errorThrow("无法创建映射，未知映射类型。type：{}", type);
            return null;
        }

    }

    /**
     * 转换Es映射结构
     *
     * @param esSyncConfig
     * @return
     */
    private EsMapping toEsMapping(EsSyncConfig esSyncConfig) {
        EsMapping esMapping = new EsMapping();
        esMapping.setDynamic("false");
        esSyncConfig.getTableList().stream().flatMap(table -> table.getFieldMapping().stream()).forEach(field ->
                esMapping.getProperties().put(
                        // 字段名称
                        field.getField(),
                        new EsMapping.MappingProperty(
                                // 字段类型
                                field.getEsTypeName(),
                                // 字段索引
                                String.valueOf(field.isIndex())
                        )
                ));
        return esMapping;
    }
}
