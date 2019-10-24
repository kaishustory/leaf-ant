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

package com.kaishustory.leafant.web.dao;

import com.kaishustory.leafant.common.model.EsSyncMappingField;
import com.kaishustory.leafant.common.model.EsSyncMappingTable;
import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.model.SyncDataSourceConfig;
import com.kaishustory.leafant.common.utils.DateUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.web.conf.JdbcConf;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 表定义Dao
 *
 * @author liguoyang
 * @create 2019-08-07 16:55
 **/
@Component
public class TableDefineDao {

    /**
     * MySQL连接管理
     */
    @Autowired
    private JdbcConf jdbcConf;

    /**
     * 读取表结构定义
     *
     * @param syncDataSourceConfig
     * @return
     */
    @SneakyThrows
    public EsSyncMappingTable getTableMapping(SyncDataSourceConfig syncDataSourceConfig) {

        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;

        EsSyncMappingTable mappingTable = new EsSyncMappingTable();
        mappingTable.setDataSourceConfig(syncDataSourceConfig);
        mappingTable.setSourceRds(syncDataSourceConfig.getRds());
        mappingTable.setSourceDatabase(syncDataSourceConfig.getDatabase());
        mappingTable.setSourceTable(syncDataSourceConfig.getTable());

        try {
            conn = jdbcConf.getConn(syncDataSourceConfig);
            // 执行查询
            statement = conn.createStatement();
            resultSet = statement.executeQuery("select * from " + syncDataSourceConfig.getTable() + " limit 0");
            ResultSetMetaData metaData = resultSet.getMetaData();

            // 读取字段结构
            List<EsSyncMappingField> mappingFields = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String column = metaData.getColumnName(i);
                int type = metaData.getColumnType(i);
                String typeName = metaData.getColumnTypeName(i);
                int size = metaData.getColumnDisplaySize(i);
                mappingFields.add(new EsSyncMappingField(
                        column,
                        size > 0 ? (typeName + "(" + size + ")").toLowerCase() : typeName.toLowerCase(),
                        type,
                        true,
                        false,
                        column
                ));
            }
            mappingTable.setFieldMapping(mappingFields);
        } catch (Exception e) {
            Log.error("数据库查询发生异常！", e);
        } finally {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        // 设置主键
        Set<String> priKeys = queryPriKeys(syncDataSourceConfig);
        mappingTable.getFieldMapping().forEach(field -> {
            if (priKeys.contains(field.getSourceColumn())) {
                field.setPrimaryKey(true);
            }
        });
        return mappingTable;
    }

    /**
     * 查询表列表
     *
     * @param syncDataSourceConfig 数据源
     * @return 表列表
     */
    @SneakyThrows
    public List<String> getTables(SyncDataSourceConfig syncDataSourceConfig) {

        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;

        List<String> tables = new ArrayList<>();
        try {
            SyncDataSourceConfig mysqlDataSource = syncDataSourceConfig.copy();
            mysqlDataSource.setDatabase("information_schema");
            conn = jdbcConf.getConn(mysqlDataSource);
            // 执行查询
            statement = conn.createStatement();
            resultSet = statement.executeQuery(String.format("select TABLE_NAME from TABLES where TABLE_SCHEMA = '%s'", syncDataSourceConfig.getDatabase()));

            while (resultSet.next()) {
                String table = resultSet.getString(1);
                tables.add(table);
            }
        } catch (Exception e) {
            Log.error("数据库查询发生异常！", e);
        } finally {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        return tables;
    }

    /**
     * 查询主键列表
     *
     * @param dataSourceConfig 数据源配置
     * @return 主键列表
     */
    private Set<String> queryPriKeys(SyncDataSourceConfig dataSourceConfig) {
        SyncDataSourceConfig newSource = dataSourceConfig.copy();
        newSource.setDatabase("information_schema");
        newSource.setTable("COLUMNS");
        return query(newSource, String.format("select COLUMN_NAME from `COLUMNS` where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' and COLUMN_KEY = 'PRI'", dataSourceConfig.getDatabase(), dataSourceConfig.getTable()))
                .stream().map(rows -> rows.get(0).getValue()).collect(Collectors.toSet());
    }

    /**
     * 数据查询
     *
     * @param dataSourceConfig 数据源配置
     * @param sql              SQL
     * @return 数据列表
     */
    @SneakyThrows
    private List<List<EventColumn>> query(SyncDataSourceConfig dataSourceConfig, String sql) {

        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            // 获得连接
            conn = jdbcConf.getConn(dataSourceConfig);
            // 执行查询
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();

            // 结果提取转换
            List<List<EventColumn>> rows = new ArrayList<>();
            while (resultSet.next()) {
                List<EventColumn> eventColumnList = new ArrayList<>(metaData.getColumnCount());
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String column = metaData.getColumnName(i);
                    int type = metaData.getColumnType(i);
                    String typeName = metaData.getColumnTypeName(i);
                    int size = metaData.getColumnDisplaySize(i);
                    String value = getColumnValue(type, resultSet, i);
                    eventColumnList.add(new EventColumn(
                            false,
                            i - 1,
                            column,
                            value,
                            size > 0 ? (typeName + "(" + size + ")").toLowerCase() : typeName.toLowerCase(),
                            type,
                            true,
                            value == null
                    ));
                }
                rows.add(eventColumnList);
            }
            return rows;

        } catch (Exception e) {
            Log.error("数据库查询发生异常！", e);
        } finally {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        return new ArrayList<>();
    }

    /**
     * 读取列值
     *
     * @param type      列类型
     * @param resultSet 数据集
     * @param i         列下标
     * @return 列值
     * @throws SQLException
     */
    private String getColumnValue(int type, ResultSet resultSet, int i) throws SQLException {
        switch (type) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIT:
                return resultSet.getString(i);
            case Types.BIGINT:
                return resultSet.getString(i);
            case Types.FLOAT:
                return resultSet.getString(i);
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return resultSet.getString(i);
            case Types.DATE:
                Date date = resultSet.getDate(i);
                return date != null ? DateUtils.toDateString(new java.util.Date(date.getTime())) : null;
            case Types.TIME:
                Time time = resultSet.getTime(i);
                return time != null ? DateUtils.toTimeString(new java.util.Date(time.getTime())) : null;
            case Types.TIMESTAMP:
                Timestamp timestamp = resultSet.getTimestamp(i);
                return timestamp != null ? DateUtils.toTimeString(new java.util.Date(timestamp.getTime())) : null;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            default:
                return resultSet.getString(i);
        }
    }

}
