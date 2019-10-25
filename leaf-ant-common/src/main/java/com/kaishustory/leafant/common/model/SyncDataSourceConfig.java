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

import java.util.Objects;

/**
 * 数据源配置
 *
 * @author liguoyang
 * @create 2019-08-06 18:29
 **/
@Data
public class SyncDataSourceConfig {

    /**
     * 数据实例名称
     */
    private String rds;
    /**
     * 数据库地址（ip:port）
     */
    private String url;
    /**
     * 数据库
     */
    private String database;
    /**
     * 表名
     */
    private String table;
    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;

    public SyncDataSourceConfig() {
    }

    public SyncDataSourceConfig(String rds, String url, String database, String table, String username, String password) {
        this.rds = rds;
        this.url = url;
        this.database = database;
        this.table = table;
        this.username = username;
        this.password = password;
    }

    public SyncDataSourceConfig copy() {
        return new SyncDataSourceConfig(rds, url, database, table, username, password);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SyncDataSourceConfig that = (SyncDataSourceConfig) o;
        return Objects.equals(rds, that.rds) &&
                Objects.equals(url, that.url) &&
                Objects.equals(database, that.database) &&
                Objects.equals(table, that.table) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rds, url, database, table, username, password);
    }
}
