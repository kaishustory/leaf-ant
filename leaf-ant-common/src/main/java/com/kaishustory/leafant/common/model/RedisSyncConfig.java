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

package com.kaishustory.leafant.common.model;

import lombok.Data;

import java.util.Date;

import static com.kaishustory.leafant.common.constants.EventConstants.LOAD_STATUS_NO;

/**
 * Redis表同步映射配置
 *
 * @author liguoyang
 * @create 2019-08-06 14:53
 **/
@Data
public class RedisSyncConfig {

    /**
     * 主键
     */
    private String id;

    /**
     * 环境
     */
    private String env;

    /**
     * 数据源实例
     */
    private String sourceRds;

    /**
     * 数据源库
     */
    private String sourceDatabase;

    /**
     * 数据源表
     */
    private String sourceTable;

    /**
     * Redis索引KEY
     */
    private String redisKeyPrefix;

    /**
     * 关联主键字段
     */
    private String joinColumn;

    /**
     * 是否同步
     */
    private boolean sync = false;

    /**
     * 初始化状态：no：未初始化，initing：初始化中，complete：完成，fail：失败
     */
    private String init = LOAD_STATUS_NO;

    /**
     * 简化字段，只保留字段名和字段值
     */
    private boolean simplifyField = true;

    /**
     * MySQL数据源配置（读取）
     */
    private SyncDataSourceConfig dataSourceConfig;

    /**
     * Redis数据源配置（写入）
     */
    private RedisDataSourceConfig redisDataSourceConfig;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;

    /**
     * 是否副本子表
     */
    public boolean isCopyChild() {
        return redisKeyPrefix.startsWith("child:");
    }

    /**
     * Redis数据源配置
     */
    @Data
    public static class RedisDataSourceConfig {

        /**
         * Redis地址（IP:端口）
         */
        private String redisAddr;
        /**
         * Redis密码
         */
        private String password;
        /**
         * DB
         */
        private int database = 0;

        public RedisDataSourceConfig() {
        }

        public RedisDataSourceConfig(String redisAddr, String password, int database) {
            this.redisAddr = redisAddr;
            this.password = password;
            this.database = database;
        }
    }
}
