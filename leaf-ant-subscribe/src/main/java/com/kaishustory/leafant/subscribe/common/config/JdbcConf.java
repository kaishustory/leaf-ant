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

package com.kaishustory.leafant.subscribe.common.config;


import com.kaishustory.leafant.common.model.SyncDataSourceConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库连接
 *
 * @author liguoyang
 * @create 2019-07-16 11:46
 **/
@Component
public class JdbcConf {

    /**
     * 连接池 Map<Host:Port:Database, DataSource>
     */
    private Map<String, HikariDataSource> connPool = new HashMap<>();

    /**
     * 最大连接数
     */
    @Value("${mysql.pool.max:10}")
    private int maxPool;

    /**
     * 获得MySQL连接
     *
     * @param dataSourceConfig 数据源配置
     * @return 连接
     * @throws SQLException
     */
    @SneakyThrows
    public Connection getConn(SyncDataSourceConfig dataSourceConfig) {
        String[] hostPort = dataSourceConfig.getUrl().split(":");
        if (hostPort.length != 2) {
            Log.errorThrow("数据库地址格式错误！{}", dataSourceConfig.getRds());
        }
        if (dataSourceConfig.getUsername() == null) {
            Log.errorThrow("数据库账号为空！{}", dataSourceConfig.getRds());
        }
        if (dataSourceConfig.getPassword() == null) {
            Log.errorThrow("数据库密码为空！{}", dataSourceConfig.getRds());
        }

        return getConn(hostPort[0], hostPort[1], dataSourceConfig.getDatabase(), dataSourceConfig.getUsername(), dataSourceConfig.getPassword());
    }

    /**
     * 获得MySQL连接
     *
     * @param host     地址
     * @param port     端口
     * @param database 数据库
     * @param user     用户
     * @param password 密码
     * @return 连接
     * @throws SQLException
     */
    @SneakyThrows
    private Connection getConn(String host, String port, String database, String user, String password) throws SQLException {
        String dataSourceKey = host + ":" + port + ":" + database;
        if (connPool.containsKey(dataSourceKey)) {
            return connPool.get(dataSourceKey).getConnection();
        } else {
            // 创建连接池
            HikariDataSource dataSource = new HikariDataSource();
            dataSource.setPoolName(database);
            dataSource.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s?characterEncoding=UTF-8&serverTimezone=Asia/Shanghai", host, port, database));
            dataSource.setUsername(user);
            dataSource.setPassword(password);
            dataSource.setConnectionTimeout(5000);
            dataSource.setMinimumIdle(1);
            dataSource.setMaximumPoolSize(maxPool);
            connPool.put(dataSourceKey, dataSource);
            return dataSource.getConnection();
        }
    }
}
