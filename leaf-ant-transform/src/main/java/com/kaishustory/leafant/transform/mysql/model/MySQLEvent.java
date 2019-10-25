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

package com.kaishustory.leafant.transform.mysql.model;

import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.model.MySQLSyncConfig;
import com.kaishustory.leafant.common.model.SyncDataSourceConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import lombok.Data;

import java.sql.Types;
import java.util.Objects;
import java.util.Optional;

/**
 * MySQL事件
 **/
@Data
public class MySQLEvent {

    /**
     * 事件信息
     */
    private Event event;

    /**
     * 目标数据源
     */
    private SyncDataSourceConfig targetDataSource;

    public MySQLEvent() {
    }

    /**
     * MySQL事件
     *
     * @param event           事件信息
     * @param mysqlSyncConfig MySQL配置
     */
    public MySQLEvent(Event event, MySQLSyncConfig mysqlSyncConfig) {
        this.event = event;
        Option<SyncDataSourceConfig> target = getTargetDataSource(event, mysqlSyncConfig);
        if (target.exist()) {
            this.targetDataSource = target.get();
        } else {
            Log.errorThrow("【MySQL】提取目标数据表时发生异常！table：{}，id：{}，errmsg：{}", mysqlSyncConfig.getSourceTable(), event.getPrimaryKey(), target.error() ? target.getErrmsg() : "");
        }
    }

    /**
     * 判断是否为数字类型
     *
     * @param type 列类型
     * @return 是否为数字类型
     */
    private static boolean isInt(int type) {
        switch (type) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIT:
            case Types.BIGINT:
                return true;
            default:
                return false;
        }
    }

    /**
     * 获得目标数据源
     *
     * @param event           事件信息
     * @param mysqlSyncConfig MySQL配置
     * @return 目标数据源
     */
    public Option<SyncDataSourceConfig> getTargetDataSource(Event event, MySQLSyncConfig mysqlSyncConfig) {

        if (mysqlSyncConfig.isSharding()) {
            // 分表字段
            Optional<EventColumn> shardingCol = event.getAllColumns().stream().filter(e -> e.getName().equals(mysqlSyncConfig.getShardingCol())).findFirst();
            if (!shardingCol.isPresent()) {
                Log.error("分表字段不存在！table：{}，column：{}", mysqlSyncConfig.getSourceTable(), mysqlSyncConfig.getShardingCol());
                return Option.empty();
            }

            // 计算分片
            long shardingIndex = sharding(shardingCol.get(), mysqlSyncConfig.getTargetDataSource().size());
            if (mysqlSyncConfig.getTargetDataSource().containsKey(shardingIndex)) {
                return Option.of(mysqlSyncConfig.getTargetDataSource().get(shardingIndex));
            } else {
                return Option.empty();
            }
        } else {
            return Option.of(mysqlSyncConfig.getTargetDataSource().values().stream().findFirst());
        }
    }

    /**
     * 计算分片
     *
     * @param shardingCol 分片字段
     * @param num         分片数量
     * @return 分片序号
     */
    private long sharding(EventColumn shardingCol, int num) {
        // 计算分片
        long shardingValue = shardingCol.getValue() != null ?
                (isInt(shardingCol.getSqlType()) ?
                        // 整数取值
                        Long.parseLong(shardingCol.getValue()) :
                        // 字符串，小数取hash值
                        Objects.hashCode(shardingCol.getValue()) & Integer.MAX_VALUE)
                : 0L;
        // 根据分片数取模
        long shardingIndex = shardingValue % num;
        return shardingIndex;
    }

}
