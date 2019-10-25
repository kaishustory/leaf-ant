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

package com.kaishustory.leafant.transform.mysql.dao;

import com.kaishustory.leafant.common.model.EventColumn;
import com.kaishustory.leafant.common.model.SyncDataSourceConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Time;
import com.kaishustory.leafant.transform.common.conf.JdbcConf;
import com.kaishustory.leafant.transform.mysql.model.MySQLEvent;
import com.mysql.jdbc.PreparedStatement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

/**
 * 数据库操作
 **/
@Component
public class MySQLDao {

    /**
     * Jdbc连接管理
     */
    @Autowired
    private JdbcConf jdbcConf;

    /**
     * 判断是否为数字类型
     *
     * @param type 列类型
     * @return 是否为数字类型
     */
    public static boolean isNum(int type) {
        switch (type) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIT:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return true;
            default:
                return false;
        }
    }

    /**
     * 批量更新操作
     *
     * @param targetDataSourceConfig 目标数据源
     * @param events                 事件列表
     */
    public void batchUpdate(SyncDataSourceConfig targetDataSourceConfig, List<MySQLEvent> events) {
        Connection mysqlConn = null;
        PreparedStatement mysqlStatm = null;
        try {
            Time time = new Time(String.format("【MySQL】批量更新 table：%s，size：%d", targetDataSourceConfig.getTable(), events.size()));
            // 获得连接
            mysqlConn = jdbcConf.getConn(targetDataSourceConfig);
            List<EventColumn> simlpeCols = events.get(0).getEvent().getAllColumns();
            // 生产执行语句
            String sql = String.format("LOAD DATA CONCURRENT LOCAL INFILE 'sql.csv' REPLACE INTO TABLE %s CHARACTER SET utf8mb4 FIELDS TERMINATED BY ' +o.o+ ' LINES TERMINATED BY ';xox\\n' (%s)", targetDataSourceConfig.getTable(), simlpeCols.stream().map(EventColumn::getName).reduce((a, b) -> a + "," + b).get());
            mysqlStatm = mysqlConn.prepareStatement(sql).unwrap(com.mysql.jdbc.PreparedStatement.class);
            // 写入数据
            mysqlStatm.setLocalInfileInputStream(getMySQLInputStream(simlpeCols, events));
            // 执行
            mysqlStatm.execute();
            events.forEach(event ->
                    Log.info("【MySQL】批量更新成功！rds：{}，database：{}，table：{}，id：{}，update：{}", targetDataSourceConfig.getRds(), targetDataSourceConfig.getDatabase(), targetDataSourceConfig.getTable(), event.getEvent().getPrimaryKey(), event.getEvent().getUpdateColumnsBase())
            );
            time.end();
        } catch (Exception e) {
            events.forEach(event ->
                    Log.error("【MySQL】批量更新失败记录！rds：{}，database：{}，table：{}，id：{}，update：{}", targetDataSourceConfig.getRds(), targetDataSourceConfig.getDatabase(), targetDataSourceConfig.getTable(), event.getEvent().getPrimaryKey(), event.getEvent().getUpdateColumnsBase())
            );
            Log.errorThrow("【MySQL】批量更新异常！rds：{}，database：{}，table：{}，size：{}，err：{}", targetDataSourceConfig.getRds(), targetDataSourceConfig.getDatabase(), targetDataSourceConfig.getTable(), events.size(), e.getMessage(), e);
        } finally {
            try {
                if (mysqlStatm != null && !mysqlStatm.isClosed()) {
                    mysqlStatm.close();
                }
                if (mysqlConn != null && !mysqlConn.isClosed()) {
                    mysqlConn.close();
                }
            } catch (Exception e) {
                Log.error("关闭MySQL连接时发生异常！", e);
            }
        }
    }

    /**
     * 批量删除操作
     *
     * @param targetDataSourceConfig 目标数据源
     * @param events                 事件列表
     */
    public void batchDelete(SyncDataSourceConfig targetDataSourceConfig, List<MySQLEvent> events) {
        Connection mysqlConn = null;
        PreparedStatement mysqlStatm = null;
        try {
            Time time = new Time(String.format("【MySQL】批量删除 table：%s，size：%d", targetDataSourceConfig.getTable(), events.size()));
            // 主键
            EventColumn pri = events.get(0).getEvent().getAllColumns().stream().filter(EventColumn::isKey).findFirst().get();
            // 获得连接
            mysqlConn = jdbcConf.getConn(targetDataSourceConfig);
            // SQL
            mysqlStatm = mysqlConn.prepareStatement(String.format("delete from %s where %s in (%s)",
                    targetDataSourceConfig.getTable(),
                    // 主键字段
                    pri.getName(),
                    // 主键值
                    events.stream()
                            .map(event -> isNum(pri.getSqlType()) ? event.getEvent().getPrimaryKey() : String.format("'%s'", event.getEvent().getPrimaryKey()))
                            .reduce((a, b) -> a + "," + b).get()
            )).unwrap(com.mysql.jdbc.PreparedStatement.class);
            // 执行命令
            mysqlStatm.execute();
            events.forEach(event ->
                    Log.info("【MySQL】批量删除成功！rds：{}，database：{}，table：{}，id：{}", targetDataSourceConfig.getRds(), targetDataSourceConfig.getDatabase(), targetDataSourceConfig.getTable(), event.getEvent().getPrimaryKey())
            );
            time.end();
        } catch (Exception e) {
            events.forEach(event ->
                    Log.error("【MySQL】批量删除失败记录！rds：{}，database：{}，table：{}，id：{}", targetDataSourceConfig.getRds(), targetDataSourceConfig.getDatabase(), targetDataSourceConfig.getTable(), event.getEvent().getPrimaryKey())
            );
            Log.errorThrow("【MySQL】批量删除异常！rds：{}，database：{}，table：{}，size：{}", targetDataSourceConfig.getRds(), targetDataSourceConfig.getDatabase(), targetDataSourceConfig.getTable(), events.size(), e);
        } finally {
            try {
                if (mysqlStatm != null && !mysqlStatm.isClosed()) {
                    mysqlStatm.close();
                }
                if (mysqlConn != null && !mysqlConn.isClosed()) {
                    mysqlConn.close();
                }
            } catch (Exception e) {
                Log.error("关闭MySQL连接时发生异常！", e);
            }
        }
    }

    /**
     * 获得输入流
     *
     * @param events 事件列表
     * @return 输入流
     */
    private InputStream getMySQLInputStream(List<EventColumn> simlpeCols, List<MySQLEvent> events) {

        String fieldSQ = " +o.o+ ";
        String lineSQ = ";xox\n";
        String nullSQ = "\\N";

        return new ByteArrayInputStream(
                events.stream().map(event ->
                                simlpeCols.stream().map(EventColumn::getName).map(col -> {
                                    Optional<EventColumn> column = event.getEvent().getAllColumns().stream().filter(eventColumn -> eventColumn.getName().equals(col)).findFirst();
                                    if (column.isPresent() && column.get().getValue() != null) {
                                        // 提取字段内容
                                        return column.get().getValue().replace(fieldSQ, "").replace(lineSQ, "");
                                    } else {
                                        // 空内容
                                        return nullSQ;
                                    }
                                    // 字段分隔
                                }).reduce((a, b) -> a + fieldSQ + b).get()
                        // 行分隔
                ).reduce((a, b) -> a + lineSQ + b).get().getBytes()
        );

    }
}
