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

package com.kaishustory.leafant.web.service;

import com.kaishustory.leafant.common.model.*;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Page;
import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponse;
import com.kaishustory.message.producer.NettyTopicProducer;
import com.kaishustory.leafant.web.dao.ElasticSearchMappingDao;
import com.kaishustory.leafant.web.dao.TableDefineDao;
import com.kaishustory.leafant.web.vo.ElasticSearchMappingVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.EventConstants.*;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_ES;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_REDIS;
import static com.kaishustory.message.common.model.RpcResponse.STATUS_SUCCESS;

/**
 * ElasticSearch映射Service
 *
 * @author liguoyang
 * @create 2019-08-02 13:57
 **/
@Service
public class ElasticSearchMappingService {

    /**
     * Es同步映射配置Dao
     */
    @Autowired
    private ElasticSearchMappingDao elasticSearchMappingDao;

    /**
     * 表定义Dao
     */
    @Autowired
    private TableDefineDao tableDefineDao;

    /**
     * 映射管理
     */
    @Resource(name = "createMappingMessageProducerObject")
    private NettyTopicProducer createMappingMessageProducer;

    /**
     * 同步状态管理
     */
    @Resource(name = "syncMessageProducerObject")
    private NettyTopicProducer syncMessageProducer;

    /**
     * 初始化数据管理
     */
    @Resource(name = "loadMessageProducerObject")
    private NettyTopicProducer loadMessageProducer;

    /**
     * 查询映射列表
     * @param page 页号
     * @param pageSize 每页条数
     * @return 映射列表
     */
    public Page<ElasticSearchMappingVo> search(String sourceTable, int page, int pageSize){
        return toVo(elasticSearchMappingDao.search(sourceTable, page, pageSize));
    }

    /**
     * 查询表定义
     * @param syncDataSourceConfig 数据源
     * @return 表定义
     */
    public EsSyncMappingTable getTableMapping(SyncDataSourceConfig syncDataSourceConfig){
        return tableDefineDao.getTableMapping(syncDataSourceConfig);
    }

    /**
     * 创建索引
     * @param esSyncConfig 索引定义
     */
    public boolean createIndex(EsSyncConfig esSyncConfig){
        // 发送创建索引消息
        RpcResponse response = createMappingMessageProducer.sendSyncMsg(new RpcRequest(TYPE_ES, JsonUtils.toJson(esSyncConfig)));
        if(response.getStatus() == STATUS_SUCCESS){
            Log.info("ES 创建索引成功。index：{}，type：{}", esSyncConfig.getIndex(), esSyncConfig.getType());
        }else {
            Log.error("ES 创建索引失败。index：{}，type：{}", esSyncConfig.getIndex(), esSyncConfig.getType());
        }
        return response.success();
    }

    /**
     * 加载初始数据
     * @param mappingId 数据同步定义ID
     */
    public boolean loadData(String mappingId){

        // 查询映射配置
        EsSyncConfig esSyncConfig = elasticSearchMappingDao.find(mappingId);

        if(esSyncConfig==null){
            Log.error("ES 配置ID不存在。{}", mappingId);
            return false;
        }

        new Thread(() -> {
            Log.info("ES 初始化开始。database：{}，table：{}", esSyncConfig.getMasterTable().getDataSourceConfig().getDatabase(), esSyncConfig.getMasterTable().getDataSourceConfig().getTable());
            // 初始化ES数据
            RpcResponse esResponse = loadMessageProducer.sendSyncMsg(new RpcRequest(ACTION_LOAD, JsonUtils.toJson(new EsInitLoadInfo(esSyncConfig))), 3, TimeUnit.HOURS);
            if(esResponse.success()){
                Log.info("ES 初始化成功。database：{}，table：{}", esSyncConfig.getMasterTable().getDataSourceConfig().getDatabase(), esSyncConfig.getMasterTable().getDataSourceConfig().getTable());
            }else {
                Log.error("ES 初始化失败。database：{}，table：{}", esSyncConfig.getMasterTable().getDataSourceConfig().getDatabase(), esSyncConfig.getMasterTable().getDataSourceConfig().getTable());

            }
        }, "es-load-monitor-thread").start();

        return true;
    }

    /**
     * 同步状态变更
     * @param mappingId 数据同步定义ID
     * @return
     */
    public void updateSyncStatus(String mappingId, boolean syncStatus){
        // 查询映射配置
        EsSyncConfig esSyncConfig = elasticSearchMappingDao.find(mappingId);
        syncMessageProducer.sendSyncMsg(new RpcRequest(TYPE_ES, JsonUtils.toJson(new SyncStatus(TYPE_ES, mappingId, syncStatus))));
        // 同步子表Redis副本状态
        if(esSyncConfig.isMult()){
            if(TYPE_REDIS.equals(esSyncConfig.getCopyChildType())) {
                esSyncConfig.getTableList().stream().filter(EsSyncMappingTable::isChild).forEach(child -> {
                    syncMessageProducer.sendSyncMsg(new RpcRequest(TYPE_REDIS, JsonUtils.toJson(new SyncStatus(TYPE_REDIS, child.getRedisMappingId(), syncStatus))));
                });
            }else  if(TYPE_ES.equals(esSyncConfig.getCopyChildType())) {
                esSyncConfig.getTableList().stream().filter(EsSyncMappingTable::isChild).forEach(child -> {
                    syncMessageProducer.sendSyncMsg(new RpcRequest(TYPE_ES, JsonUtils.toJson(new SyncStatus(TYPE_ES, child.getEsCopyMappingId(), syncStatus))));
                });
            }
        }
    }

    /**
     * 转换Vo
     * @param domains
     * @return
     */
    private Page<ElasticSearchMappingVo> toVo(Page<EsSyncConfig> domains){
        return Page.of(domains.getResult().stream().map(mapping -> {
            List<ElasticSearchMappingVo.ElasticSearchMappingSourceVo> sourceList = mapping.getTableList().stream().map(table ->
                    new ElasticSearchMappingVo.ElasticSearchMappingSourceVo(table.isMaster(), table.getSourceRds()+"."+table.getSourceDatabase(), table.getSourceTable())
            ).collect(Collectors.toList());
            return new ElasticSearchMappingVo(mapping.getId(), mapping.getIndex()+"/"+mapping.getType(), sourceList, mapping.isSync(), mapping.getInit());
        }).collect(Collectors.toList()), domains.getTotalCount(), domains.getCurrPage(), domains.getPageSize());
    }



}
