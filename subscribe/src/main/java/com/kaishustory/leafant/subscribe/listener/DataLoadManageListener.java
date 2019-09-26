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

package com.kaishustory.leafant.subscribe.listener;

import com.kaishustory.leafant.common.model.*;
import com.kaishustory.leafant.common.utils.JsonUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.mapping.service.EsMappingService;
import com.kaishustory.leafant.subscribe.dao.DataLoadDao;
import com.kaishustory.leafant.subscribe.service.MqSendService;
import com.kaishustory.message.common.model.RpcResponse;
import com.kaishustory.message.consumer.NettyConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.EventConstants.*;
import static com.kaishustory.leafant.common.constants.MappingConstants.*;

/**
 * 数据全量加载，事件监听
 *
 * @author liguoyang
 * @create 2019-07-17 19:25
 **/
@Slf4j
@Service
public class DataLoadManageListener {

    /**
     * MQ消息发送
     */
    @Autowired
    private MqSendService mqSendService;

    /**
     * 数据初始化加载Dao
     */
    @Autowired
    private DataLoadDao dataLoadDao;

    /**
     * 同步消息Group
     */
    @Value("${message.group}")
    private String messageGroup;

    /**
     * 初始加载事件Topic
     */
    @Value("${load.topic}")
    private String loadTopic;

    /**
     * Zookeeper地址
     */
    @Value("${zookeeper.url}")
    private String zookeeper;


    /**
     * 初始数据加载事件监听
     */
    @PostConstruct
    public void loadLister(){
        // 监听同步映射配置消息
        new NettyConsumer(messageGroup, loadTopic, zookeeper, rpcRequest -> {

            // 数据初始化消息
            if(ACTION_LOAD.equals(rpcRequest.getAction())) {
                try {
                    InitLoadInfo baseInitLoad = JsonUtils.fromJson(rpcRequest.getData(), InitLoadInfo.class);

                    boolean loadSuccess;
                    // ES 初始化
                    if(TYPE_ES.equals(baseInitLoad.getTarget())) {
                        EsInitLoadInfo esInitLoadInfo = JsonUtils.fromJson(rpcRequest.getData(), EsInitLoadInfo.class);
                        loadSuccess = esInitLoad(esInitLoadInfo);
                    }else {
                        // 其他数据源 初始化
                        loadSuccess = commonInitLoad(baseInitLoad, false);
                    }
                    if(loadSuccess){
                        return new RpcResponse("load-callback", "ok", RpcResponse.STATUS_SUCCESS);
                    }else {
                        return new RpcResponse("load-callback", "fail", RpcResponse.STATUS_FAIL);
                    }
                } catch (Exception e) {
                    Log.error("初始化同步异常！", e);
                    return new RpcResponse("load-callback", "fail", RpcResponse.STATUS_FAIL);
                }

            }else {
                Log.error("无法处理消息类型：{}", rpcRequest.getAction());
                return RpcResponse.NO_REPLY;
            }
        });

    }

    /**
     * ES：初始化全量数据加载
     * @param esInitLoadInfo 初始加载信息
     */
    private boolean esInitLoad(EsInitLoadInfo esInitLoadInfo){

        EsSyncConfig esSyncConfig = esInitLoadInfo.getEsSyncConfig();

        // 初始化中
        dataLoadDao.updateInitialized(new LoadStatus(TYPE_ES, esSyncConfig.getId(), LOAD_STATUS_INITING));

        // 初始化子表数据副本
        boolean childResult = loadChildCopyData(esSyncConfig);

        // 初始化 ES数据
        if(childResult){
            // 初始化ES数据
            return commonInitLoad(new InitLoadInfo(TYPE_ES, esSyncConfig.getId(), esSyncConfig.getMasterTable().getDataSourceConfig()), true);
        }else {
            return false;
        }
    }

    /**
     * 通用：初始化全量数据加载
     * @param initLoadInfo 初始加载信息
     * @param master
     */
    private boolean commonInitLoad(InitLoadInfo initLoadInfo, boolean master){
        if(!master) {
            // 初始化中
            dataLoadDao.updateInitialized(new LoadStatus(initLoadInfo.getTarget(), initLoadInfo.getMappingId(), LOAD_STATUS_INITING));
        }
        Log.info("{} 初始化数据开始。database：{}，table：{}", initLoadInfo.getTarget(), initLoadInfo.getDataSourceConfig().getDatabase(), initLoadInfo.getDataSourceConfig().getTable());
        try {
            // 同步数据
            boolean loadSuccess = dataLoadDao.queryAllDataHandle(initLoadInfo, (rows, page, pageSize) -> {
                Log.info("初始化全量数据：RdsKey：{}，Database：{}，Table：{}，Page：{}/{}，Size：{}", initLoadInfo.getDataSourceConfig().getRds(), initLoadInfo.getDataSourceConfig().getDatabase(), initLoadInfo.getDataSourceConfig().getTable(), page, pageSize, rows==null? 0:rows.size());
                return mqSendService.send(rows.stream().map(row -> new Event("MYSQL", SOURCE_INIT, initLoadInfo.getTarget(), initLoadInfo.getMappingId(), initLoadInfo.getDataSourceConfig().getRds(), initLoadInfo.getDataSourceConfig().getDatabase(), initLoadInfo.getDataSourceConfig().getTable(), 1, "INSERT", getPrimaryKey(row), new ArrayList<>(), row, System.currentTimeMillis())).collect(Collectors.toList()));
            });
            if(loadSuccess) {
                // 初始化成功
                dataLoadDao.updateInitialized(new LoadStatus(initLoadInfo.getTarget(), initLoadInfo.getMappingId(), LOAD_STATUS_COMPLETE));
                Log.info("{} 初始化数据成功。database：{}，table：{}", initLoadInfo.getTarget(), initLoadInfo.getDataSourceConfig().getDatabase(), initLoadInfo.getDataSourceConfig().getTable());
            }else {
                Log.error("{} 初始化数据失败。database：{}，table：{}", initLoadInfo.getTarget(), initLoadInfo.getDataSourceConfig().getDatabase(), initLoadInfo.getDataSourceConfig().getTable());
                // 初始化失败
                dataLoadDao.updateInitialized(new LoadStatus(initLoadInfo.getTarget(), initLoadInfo.getMappingId(), LOAD_STATUS_FAIL));
            }
            return loadSuccess;
        }catch (Exception e){
            Log.error("{} 初始化数据失败。database：{}，table：{}", initLoadInfo.getTarget(), initLoadInfo.getDataSourceConfig().getDatabase(), initLoadInfo.getDataSourceConfig().getTable(), e);
            // 初始化失败
            dataLoadDao.updateInitialized(new LoadStatus(initLoadInfo.getTarget(), initLoadInfo.getMappingId(), LOAD_STATUS_FAIL));
            return false;
        }
    }

    /**
     * 初始化子表副本数据
     * @param esSyncConfig ES配置
     * @return 是否成功
     */
    private boolean loadChildCopyData(EsSyncConfig esSyncConfig){

        if(!esSyncConfig.isMult() || esSyncConfig.getMasterTable().getChildTable().size()==0){
            return true;
        }

        if(TYPE_REDIS.equals(esSyncConfig.getCopyChildType())) {
            // 初始化Redis 副本数据
            return esSyncConfig.getTableList().stream().filter(EsSyncMappingTable::isChild).map(table -> {
                // Redis副本，初始数据消息
                return commonInitLoad(new InitLoadInfo(TYPE_REDIS, table.getRedisMappingId(), table.getDataSourceConfig()), false);
            }).reduce((a, b) -> a && b).orElse(true);

        }else if(TYPE_ES.equals(esSyncConfig.getCopyChildType())){
            // 初始化ES 副本数据
            return esSyncConfig.getMasterTable().getChildTable().stream().map(table -> {
                // ES副本，初始数据消息
                return esInitLoad(new EsInitLoadInfo(EsMappingService.toChildEsConfig(table, esSyncConfig.getIndex(), esSyncConfig.getEsAddr())));
            }).reduce((a, b) -> a && b).orElse(true);

        }else {
            Log.errorThrow("不支持的副本子表类型：{}", esSyncConfig.getCopyChildType());
            return false;
        }
    }

    /**
     * 获得主键值
     * @param row 列集合
     * @return 主键值
     */
    private String getPrimaryKey(List<EventColumn> row){
        return row.stream().filter(EventColumn::isKey).map(EventColumn::getValue).reduce((a, b) -> a+":"+b).orElse("");
    }



}
