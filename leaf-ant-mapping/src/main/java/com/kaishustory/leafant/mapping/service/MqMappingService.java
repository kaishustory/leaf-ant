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

import com.kaishustory.leafant.common.model.LoadStatus;
import com.kaishustory.leafant.common.model.MqSyncConfig;
import com.kaishustory.leafant.common.model.SyncStatus;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.mapping.dao.MqMappingDao;
import com.kaishustory.leafant.mapping.service.interfaces.IMappingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import static com.kaishustory.leafant.common.constants.MappingConstants.TYPE_MQ;

/**
 * MQ映射管理
 *
 * @author liguoyang
 * @create 2019-08-21 15:29
 **/
@Service
@ConditionalOnProperty(name = "message.mapping.producer", havingValue = "true")
public class MqMappingService implements IMappingService {

    /**
     * MQ映射管理
     */
    @Autowired
    private MqMappingDao mqMappingDao;

    /**
     * 配置同步管理
     */
    @Autowired
    private MappingSyncService mappingSyncService;

    /**
     * 创建MQ映射
     *
     * @param mqSyncConfig 同步映射
     * @return 新增映射ID
     */
    public Option<String> createIndex(MqSyncConfig mqSyncConfig) {

        // 保存同步映射配置
        Option<String> mappingId = mqMappingDao.saveConfig(mqSyncConfig);
        if (!mappingId.exist()) {
            return Option.error("MQ 保存Mongo映射失败！");
        }
        // 通知同步映射更新
        boolean sync = mappingSyncService.sync(TYPE_MQ);
        if (!sync) {
            return Option.error("MQ 同步映射配置失败！");
        }
        Log.info("MQ 创建映射成功！table：{}", mqSyncConfig.getSourceTable());
        return mappingId;
    }

    /**
     * 更改已初始化状态
     *
     * @param loadStatus 状态
     */
    @Override
    public void updateInitialized(LoadStatus loadStatus) {
        mqMappingDao.updateInitialized(loadStatus);
        Log.info("MQ 更新初始化状态！mappingId：{}，loadStatus：{}", loadStatus.getMappingId(), loadStatus.getLoadStatus());
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
        mqMappingDao.updateSync(syncStatus);
        // 通知同步映射更新
        if (!mappingSyncService.sync(TYPE_MQ)) {
            return Option.error("MQ 同步映射配置失败！");
        }
        Log.info("MQ 更新同步状态成功！mappingId：{}，syncStatus：{}", syncStatus.getMappingId(), syncStatus.isSync());
        return Option.of("OK");
    }
}
