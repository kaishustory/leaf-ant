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

import com.kaishustory.leafant.common.model.InitLoadInfo;
import com.kaishustory.leafant.common.model.MySQLSyncConfig;
import com.kaishustory.leafant.common.model.SyncStatus;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Page;
import com.kaishustory.leafant.web.dao.MySQLMappingDao;
import com.kaishustory.message.common.model.RpcRequest;
import com.kaishustory.message.common.model.RpcResponse;
import com.kaishustory.message.producer.NettyTopicProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.kaishustory.leafant.common.constants.EventConstants.ACTION_LOAD;
import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_MYSQL;
import static com.kaishustory.message.common.model.RpcResponse.STATUS_SUCCESS;

/**
 * MySQL映射Service
 *
 * @author liguoyang
 * @create 2019-08-02 13:57
 **/
@Service
public class MySQLMappingService {

    /**
     * MySQL同步映射配置Dao
     */
    @Autowired
    private MySQLMappingDao mysqlMappingDao;

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
     *
     * @param page     页号
     * @param pageSize 每页条数
     * @return 映射列表
     */
    public Page<MySQLSyncConfig> search(String sourceTable, int page, int pageSize) {
        return toVo(mysqlMappingDao.search(sourceTable, page, pageSize));
    }

    /**
     * 转换Vo
     *
     * @param domains
     * @return
     */
    private Page<MySQLSyncConfig> toVo(Page<MySQLSyncConfig> domains) {
        return domains;
    }

    /**
     * 创建映射
     *
     * @param mysqlSyncConfig 映射定义
     */
    public boolean createMapping(MySQLSyncConfig mysqlSyncConfig) {

        // 发送创建映射消息
        RpcResponse response = createMappingMessageProducer.sendSyncMsg(new RpcRequest(TYPE_MYSQL, JsonUtils.toJson(mysqlSyncConfig)));
        if (response.getStatus() == STATUS_SUCCESS) {
            Log.info("MySQL 创建映射成功。table：{}", mysqlSyncConfig.getSourceTable());
            return true;
        } else {
            Log.error("MySQL 创建映射失败。table：{}", mysqlSyncConfig.getSourceTable());
            return false;
        }
    }

    /**
     * 加载初始数据
     *
     * @param mappingId 数据同步定义ID
     */
    public boolean loadData(String mappingId) {

        // 查询映射配置
        MySQLSyncConfig mysqlSyncConfig = mysqlMappingDao.find(mappingId);

        if (mysqlSyncConfig == null) {
            Log.error("MySQL 配置ID不存在。{}", mappingId);
            return false;
        }

        new Thread(() -> {
            Log.info("MySQL 初始化开始。database：{}，table：{}", mysqlSyncConfig.getSourceDatabase(), mysqlSyncConfig.getSourceTable());
            // 发送初始数据消息
            RpcResponse response = loadMessageProducer.sendSyncMsg(new RpcRequest(ACTION_LOAD, JsonUtils.toJson(new InitLoadInfo(TYPE_MYSQL, mappingId, mysqlSyncConfig.getSourceDataSource()))), 3, TimeUnit.HOURS);
            if (response.getStatus() == STATUS_SUCCESS) {
                Log.info("MySQL 初始化数据成功。database：{}，table：{}", mysqlSyncConfig.getSourceDatabase(), mysqlSyncConfig.getSourceTable());
            } else {
                Log.error("MySQL 初始化数据失败。database：{}，table：{}", mysqlSyncConfig.getSourceDatabase(), mysqlSyncConfig.getSourceTable());
            }
        }, "mysql-load-monitor-thread").start();

        // 返回消息发送成功
        return true;
    }

    /**
     * 同步状态变更
     *
     * @param mappingId 数据同步定义ID
     * @return
     */
    public void updateSyncStatus(String mappingId, boolean syncStatus) {
        syncMessageProducer.sendSyncMsg(new RpcRequest(TYPE_MYSQL, JsonUtils.toJson(new SyncStatus(TYPE_MYSQL, mappingId, syncStatus))));
    }


}
