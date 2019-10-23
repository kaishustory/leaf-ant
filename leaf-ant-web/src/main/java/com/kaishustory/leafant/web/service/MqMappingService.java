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
import com.kaishustory.leafant.web.dao.MqMappingDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;


import java.util.concurrent.TimeUnit;

import static com.kaishustory.leafant.common.constants.EventConstants.*;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_MQ;
import static com.kaishustory.message.common.model.RpcResponse.STATUS_SUCCESS;

/**
 * MQ映射管理
 *
 * @author liguoyang
 * @create 2019-08-28 15:04
 **/
@Service
public class MqMappingService {

    /**
     * MQ同步映射配置Dao
     */
    @Autowired
    private MqMappingDao mqMappingDao;

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
    public Page<MqSyncConfig> search(String sourceTable, int page, int pageSize){
        return toVo(mqMappingDao.search(sourceTable, page, pageSize));
    }

    /**
     * 转换Vo
     * @param domains
     * @return
     */
    private Page<MqSyncConfig> toVo(Page<MqSyncConfig> domains){
        return domains;
    }

    /**
     * 创建映射
     * @param mqSyncConfig 映射定义
     */
    public boolean createMapping(MqSyncConfig mqSyncConfig){

        // 发送创建映射消息
        RpcResponse response = createMappingMessageProducer.sendSyncMsg(new RpcRequest(TYPE_MQ, JsonUtils.toJson(mqSyncConfig)));
        if(response.getStatus() == STATUS_SUCCESS){
            Log.info("MQ 创建映射成功。table：{}", mqSyncConfig.getSourceTable());
            return true;
        }else {
            Log.error("MQ 创建映射失败。table：{}", mqSyncConfig.getSourceTable());
            return false;
        }
    }

    /**
     * 加载初始数据
     * @param mappingId 数据同步定义ID
     */
    public boolean loadData(String mappingId){

        // 查询映射配置
        MqSyncConfig mqSyncConfig = mqMappingDao.find(mappingId);

        if(mqSyncConfig==null){
            Log.error("MQ 配置ID不存在。{}", mappingId);
            return false;
        }
        if(LOAD_STATUS_NO_SUPPORT.equals(mqSyncConfig.getInit())){
            Log.warn("MQ 不支持初始化，无法进行初始化导入。{}", mappingId);
        }

        new Thread(() -> {
            Log.info("MQ 初始化开始。database：{}，table：{}", mqSyncConfig.getDataSourceConfig().getDatabase(), mqSyncConfig.getDataSourceConfig().getTable());
            // 发送初始数据消息
            RpcResponse response = loadMessageProducer.sendSyncMsg(new RpcRequest(ACTION_LOAD, JsonUtils.toJson(new InitLoadInfo( TYPE_MQ, mappingId, mqSyncConfig.getDataSourceConfig()))), 3, TimeUnit.HOURS);
            if(response.getStatus() == STATUS_SUCCESS){
                Log.info("MQ 初始化数据成功。database：{}，table：{}", mqSyncConfig.getDataSourceConfig().getDatabase(), mqSyncConfig.getDataSourceConfig().getTable());
            }else {
                Log.error("MQ 初始化数据失败。database：{}，table：{}", mqSyncConfig.getDataSourceConfig().getDatabase(), mqSyncConfig.getDataSourceConfig().getTable());
            }
        }, "mq-load-monitor-thread").start();

        // 返回消息发送成功
        return true;
    }

    /**
     * 同步状态变更
     * @param mappingId 数据同步定义ID
     * @return
     */
    public void updateSyncStatus(String mappingId, boolean syncStatus){
        syncMessageProducer.sendSyncMsg(new RpcRequest(TYPE_MQ, JsonUtils.toJson(new SyncStatus(TYPE_MQ, mappingId, syncStatus))));
    }
}
