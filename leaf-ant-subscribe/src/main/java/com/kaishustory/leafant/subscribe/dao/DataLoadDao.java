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

package com.kaishustory.leafant.subscribe.dao;

import com.kaishustory.leafant.common.model.*;
import com.kaishustory.leafant.common.utils.DateUtils;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.mapping.dao.*;
import com.kaishustory.leafant.mapping.model.LoadRecord;
import com.kaishustory.leafant.mapping.model.LoadStats;
import com.kaishustory.leafant.subscribe.common.config.JdbcConf;
import com.kaishustory.leafant.subscribe.model.PriColInfo;
import com.kaishustory.leafant.subscribe.model.TablePageInfo;
import lombok.SneakyThrows;
import org.apache.commons.lang.time.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.MappingConstants.*;
import static com.kaishustory.leafant.mapping.model.LoadRecord.*;


/**
 * 数据初始化加载Dao
 **/
@Component
public class DataLoadDao {

    /**
     * MySQL连接
     */
    @Autowired
    private JdbcConf jdbcConf;

    /**
     * 初始化记录
     */
    @Autowired
    private LoadRecordDao loadRecordDao;

    /**
     * Es映射
     */
    @Autowired
    private EsMappingDao esMappingDao;

    /**
     * Redis映射
     */
    @Autowired
    private RedisMappingDao redisMappingDao;

    /**
     * MQ映射
     */
    @Autowired
    private MqMappingDao mqMappingDao;

    /**
     * MySQL映射
     */
    @Autowired
    private MySQLMappingDao mysqlMappingDao;

    /**
     * 初始化线程最大线程数
     */
    @Value("${load.max-pool:1}")
    private int loadMaxPool;

    private ThreadPoolExecutor threadPool;

    private ThreadPoolExecutor getThreadPool() {
        synchronized (this) {
            if (threadPool == null) {
                synchronized (this) {
                    threadPool = new ThreadPoolExecutor(loadMaxPool, loadMaxPool, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(Integer.MAX_VALUE));
                }
            }
        }
        return threadPool;
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
     * 查询全部数据，分页处理
     *
     * @param initLoadInfo   初始化信息
     * @param dataLoadHandle 数据处理
     */
    @SneakyThrows
    public boolean queryAllDataHandle(InitLoadInfo initLoadInfo, DataLoadHandle dataLoadHandle) {

        // 查询主键列表
        Optional<PriColInfo> prikey = queryPriKeys(initLoadInfo.getDataSourceConfig());

        if (prikey.isPresent()) {

            // 删除主键自增
            if (TYPE_MYSQL.equals(initLoadInfo.getTarget())) {
                removeAutoIncrement(initLoadInfo.getMappingId(), prikey.get());
            }

            // 主键类型是否位数字
            if (prikey.get().isNum()) {
                /** 按主键为分页条件，分页查询 **/
                idPageHandler(initLoadInfo, prikey.get(), dataLoadHandle);

            } else {
                /** 按序号为分页条件，分页查询 **/
                limitPageHandler(initLoadInfo, prikey.get(), dataLoadHandle);
            }

            // 等待完成
            boolean ok = waitComplete(initLoadInfo);

            // 添加主键自增
            if (TYPE_MYSQL.equals(initLoadInfo.getTarget())) {
                addAutoIncrement(initLoadInfo.getMappingId(), prikey.get());
            }

            return ok;

        } else {
            Log.errorThrow("不存在主键，无法同步数据。table：{}", initLoadInfo.getDataSourceConfig().getTable());
            return false;
        }

    }

    /**
     * ID模式分页查询处理
     *
     * @param initLoadInfo   初始化信息
     * @param prikey         主键信息
     * @param dataLoadHandle 分页处理
     */
    private void idPageHandler(InitLoadInfo initLoadInfo, PriColInfo prikey, DataLoadHandle dataLoadHandle) {

        // 查询记录总数
        TablePageInfo pageInfo = queryAllDataCount(initLoadInfo.getDataSourceConfig(), initLoadInfo.getDataSourceConfig().getTable(), prikey.getCol());
        int querySize = 1000;
        int mqSize = 100;
        int pageSize = (int) Math.ceil((pageInfo.getMaxKey() - pageInfo.getMinKey()) / (float) mqSize);

        // 将所有任务放入任务队列
        Deque<LoadRecord> loadRecordQueue = new LinkedBlockingDeque<>();
        for (int i = 0; i < pageSize; i++) {

            // 查询任务是否存在
            LoadRecord loadRecord = loadRecordDao.findRecordByPage(MODE_ID, initLoadInfo.getTarget(), initLoadInfo.getMappingId(), initLoadInfo.getDataSourceConfig().getTable(), i + 1, pageSize);

            if (loadRecord == null) {
                // 保存初始化记录
                loadRecord = loadRecordDao.addRecord(
                        initLoadInfo.getTarget(),
                        initLoadInfo.getMappingId(),
                        MODE_ID,
                        initLoadInfo.getDataSourceConfig().getRds(),
                        initLoadInfo.getDataSourceConfig().getDatabase(),
                        initLoadInfo.getDataSourceConfig().getTable(),
                        pageInfo.getMinKey() + (i * mqSize),
                        pageInfo.getMinKey() + ((i + 1) * mqSize),
                        i + 1,
                        pageSize
                );
                Log.info("创建初始化任务。table：{}，page：{}", initLoadInfo.getDataSourceConfig().getTable(), i + 1);
            }

            // 只有等待处理和处理失败的任务，会被加入处理队列
            if (STATUS_WAIT.equals(loadRecord.getStatus()) || STATUS_FAIL.equals(loadRecord.getStatus())) {
                loadRecordQueue.addFirst(loadRecord);
            } else {
                Log.info("已处理任务跳过。table：{}，page：{}", initLoadInfo.getDataSourceConfig().getTable(), i + 1);
            }
        }

        do {
            // 按照查询分页要求，将多个任务聚合到一起
            List<LoadRecord> loadRecordList = findTaskList(loadRecordQueue, (int) Math.ceil(querySize / mqSize));

            if (loadRecordList.size() > 0) {
                getThreadPool().execute(() -> {
                    try {
                        com.kaishustory.leafant.common.utils.Time time = new com.kaishustory.leafant.common.utils.Time("按ID分页初始化");
                        // 分页查询数据
                        List<List<EventColumn>> allRows = queryData(initLoadInfo.getDataSourceConfig(), initLoadInfo.getDataSourceConfig().getTable(), prikey.getCol(), loadRecordList.get(0).getStart(), loadRecordList.get(loadRecordList.size() - 1).getEnd());
                        // 设置主键标记
                        allRows.forEach(row -> row.forEach(column -> {
                            if (prikey.getCol().equals(column.getName())) {
                                column.setKey(true);
                            }
                        }));

                        // 按MQ分页要求，重新分页
                        idTaskHandle(loadRecordList, allRows, prikey.getCol(), dataLoadHandle);

                        time.end();
                    } catch (Throwable t) {
                        Log.error("初始化发生异常！", t);
                    }
                });
            }
        } while (loadRecordQueue.size() > 0);
    }

    /**
     * Limit模式分页查询处理
     *
     * @param initLoadInfo   初始化信息
     * @param prikey         主键信息
     * @param dataLoadHandle 分页处理
     */
    private void limitPageHandler(InitLoadInfo initLoadInfo, PriColInfo prikey, DataLoadHandle dataLoadHandle) {

        // 查询记录总数
        TablePageInfo pageInfo = queryAllDataCount(initLoadInfo.getDataSourceConfig(), initLoadInfo.getDataSourceConfig().getTable(), prikey.getCol());

        int querySize = 1000;
        int mqSize = 100;
        int pageSize = (int) Math.ceil(pageInfo.getTotal() / (float) mqSize);

        // 将所有任务放入任务队列
        Deque<LoadRecord> loadRecordQueue = new LinkedBlockingDeque<>();
        for (int i = 0; i < pageSize; i++) {

            // 查询任务是否存在
            LoadRecord loadRecord = loadRecordDao.findRecordByPage(MODE_LIMIT, initLoadInfo.getTarget(), initLoadInfo.getMappingId(), initLoadInfo.getDataSourceConfig().getTable(), i + 1, pageSize);

            if (loadRecord == null) {
                // 保存初始化记录
                loadRecord = loadRecordDao.addRecord(
                        initLoadInfo.getTarget(),
                        initLoadInfo.getMappingId(),
                        MODE_LIMIT,
                        initLoadInfo.getDataSourceConfig().getRds(),
                        initLoadInfo.getDataSourceConfig().getDatabase(),
                        initLoadInfo.getDataSourceConfig().getTable(),
                        i * mqSize, (i + 1) * mqSize,
                        i + 1,
                        pageSize
                );
                Log.info("创建初始化任务。table：{}，page：{}", initLoadInfo.getDataSourceConfig().getTable(), i + 1);
            }

            // 只有等待处理和处理失败的任务，会被加入处理队列
            if (STATUS_WAIT.equals(loadRecord.getStatus()) || STATUS_FAIL.equals(loadRecord.getStatus())) {
                loadRecordQueue.addFirst(loadRecord);
            } else {
                Log.info("已处理任务跳过。table：{}，page：{}", initLoadInfo.getDataSourceConfig().getTable(), i + 1);
            }
        }

        do {
            // 按照查询分页要求，将多个任务聚合到一起
            List<LoadRecord> loadRecordList = findTaskList(loadRecordQueue, (int) Math.ceil(querySize / mqSize));

            if (loadRecordList.size() > 0) {
                getThreadPool().execute(() -> {
                    try {
                        com.kaishustory.leafant.common.utils.Time time = new com.kaishustory.leafant.common.utils.Time("按序号分页初始化");
                        // 分页查询数据
                        List<List<EventColumn>> allRows = queryData(initLoadInfo.getDataSourceConfig(), initLoadInfo.getDataSourceConfig().getTable(), loadRecordList.get(0).getStart(), (int) (loadRecordList.get(loadRecordList.size() - 1).getEnd() - loadRecordList.get(0).getStart()));

                        // 设置主键标记
                        allRows.forEach(row -> row.forEach(column -> {
                            if (prikey.getCol().equals(column.getName())) {
                                column.setKey(true);
                            }
                        }));

                        // 按MQ分页要求，重新分页
                        limitTaskHandle(loadRecordList, allRows, mqSize, dataLoadHandle);

                        time.end();
                    } catch (Throwable t) {
                        Log.error("初始化发生异常！", t);
                    }
                });
            }
        } while (loadRecordQueue.size() > 0);
    }

    /**
     * 等待完成
     *
     * @param initLoadInfo 初始化信息
     */
    @SneakyThrows
    private boolean waitComplete(InitLoadInfo initLoadInfo) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // 等待任务处理结果
        while (true) {
            LoadStats stats = loadRecordDao.findLoadStats(initLoadInfo.getTarget(), initLoadInfo.getMappingId(), initLoadInfo.getDataSourceConfig().getTable());
            if (stats.getWait() == 0L && stats.getSend() == 0L) {
                if (stats.getFail() == 0L) {
                    Log.info("初始化导入数据成功！target：{}，table：{}，size：{}", initLoadInfo.getTarget(), initLoadInfo.getDataSourceConfig().getTable(), stats.getSuceess());
                    return true;
                } else {
                    Log.error("初始化导入数据完成，但有部分失败！target：{}，table：{}，success：{}，fail：{}", initLoadInfo.getTarget(), initLoadInfo.getDataSourceConfig().getTable(), stats.getSuceess(), stats.getFail());
                    return false;
                }
            }
            if (stopWatch.getTime() > 24 * 60 * 60 * 1000L) {
                Log.error("初始化导入数据超时，停止等待任务完成！target：{}，table：{}，success：{}，fail：{}", initLoadInfo.getTarget(), initLoadInfo.getDataSourceConfig().getTable(), stats.getSuceess(), stats.getFail());
                return false;
            }
            Thread.sleep(1000);
        }
    }

    /**
     * 获得任务
     *
     * @param loadRecordQueue 初始化计划队列
     * @param num             任务数量
     * @return 任务
     */
    private List<LoadRecord> findTaskList(Deque<LoadRecord> loadRecordQueue, int num) {
        // 按照查询分页要求，将多个任务聚合到一起
        List<LoadRecord> loadRecordList = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            LoadRecord loadRecord = loadRecordQueue.pollLast();
            if (loadRecord != null) {

                // 检查分页是否连续，不连续的分页不放在同一任务组内
                if (loadRecordList.size() > 0 && loadRecordList.get(loadRecordList.size() - 1).getPage() + 1 != loadRecord.getPage()) {
                    loadRecordQueue.addLast(loadRecord);
                    return loadRecordList;
                } else {
                    loadRecordList.add(loadRecord);
                }
            } else {
                return loadRecordList;
            }
        }
        return loadRecordList;
    }

    /**
     * ID 处理任务
     *
     * @param loadRecordList 任务列表
     * @param allRows        所有记录
     * @param priCol         主键字段
     * @param dataLoadHandle MQ处理
     */
    private void idTaskHandle(List<LoadRecord> loadRecordList, List<List<EventColumn>> allRows, String priCol, DataLoadHandle dataLoadHandle) {
        // 按MQ分页要求，重新分页
        for (int i = 0; i < loadRecordList.size(); i++) {
            LoadRecord loadRecord = loadRecordList.get(i);
            try {
                // 截取MQ分页
                List<List<EventColumn>> rows = allRows.stream().filter(row -> {
                    Optional<EventColumn> key = row.stream().filter(col -> priCol.equals(col.getName())).findFirst();
                    return key.filter(eventColumn ->
                            loadRecord.getStart() <= Long.parseLong(eventColumn.getValue()) && loadRecord.getEnd() > Long.parseLong(eventColumn.getValue())
                    ).isPresent();
                }).collect(Collectors.toList());

                if (rows.size() > 0) {
                    // 分页处理
                    List<String> mqid = dataLoadHandle.run(rows, loadRecord.getPage(), loadRecord.getPageSize());
                    // 更新已发送状态
                    loadRecordDao.updateRecordSendMqidById(loadRecord.getId(), mqid, rows.size());
                } else {
                    // 无数据直接标记成功
                    loadRecordDao.updateRecordSuccessById(loadRecord.getId());
                }
            } catch (Exception e) {
                loadRecordDao.updateRecordFailById(loadRecord.getId(), e.getMessage());
                Log.error("初始化失败！record：{}", loadRecord, e);
            }
        }
    }

    /**
     * Limit 处理任务
     *
     * @param loadRecordList 任务列表
     * @param allRows        所有记录
     * @param mqSize         MQ处理数量
     * @param dataLoadHandle MQ处理
     */
    private void limitTaskHandle(List<LoadRecord> loadRecordList, List<List<EventColumn>> allRows, int mqSize, DataLoadHandle dataLoadHandle) {
        // 按MQ分页要求，重新分页
        for (int i = 0; i < loadRecordList.size(); i++) {
            LoadRecord loadRecord = loadRecordList.get(i);
            try {
                // 截取MQ分页
                List<List<EventColumn>> rows = allRows.stream().skip(i * mqSize).limit(mqSize).collect(Collectors.toList());

                if (rows.size() > 0) {
                    // 分页处理
                    List<String> mqid = dataLoadHandle.run(rows, loadRecord.getPage(), loadRecord.getPageSize());
                    // 更新已发送状态
                    loadRecordDao.updateRecordSendMqidById(loadRecord.getId(), mqid, rows.size());
                } else {
                    // 无数据直接标记成功
                    loadRecordDao.updateRecordSuccessById(loadRecord.getId());
                }
            } catch (Exception e) {
                loadRecordDao.updateRecordFailById(loadRecord.getId(), e.getMessage());
                Log.error("初始化失败！record：{}", loadRecord, e);
            }
        }
    }

    /**
     * 查询记录数据
     *
     * @param dataSourceConfig 数据源
     * @param table            表名
     * @param start            起始行号
     * @param size             查询数量
     * @return 数据记录
     */
    private List<List<EventColumn>> queryData(SyncDataSourceConfig dataSourceConfig, String table, long start, int size) {
        return query(dataSourceConfig, String.format("select * from %s limit %d,%d", table, start, size));
    }

    /**
     * 查询记录数据
     *
     * @param dataSourceConfig 数据源
     * @param table            表名
     * @param primaryKey       主键
     * @param startKey         起始主键
     * @param endKey           截止主键
     * @return 数据记录
     */
    private List<List<EventColumn>> queryData(SyncDataSourceConfig dataSourceConfig, String table, String primaryKey, long startKey, long endKey) {
        return query(dataSourceConfig, String.format("select * from %s where %s >= %d and %s < %d", table, primaryKey, startKey, primaryKey, endKey));
    }

    /**
     * 查询记录总数
     *
     * @param dataSourceConfig 数据源配置
     * @param primaryKey       主键
     * @return 记录总数
     */
    private TablePageInfo queryAllDataCount(SyncDataSourceConfig dataSourceConfig, String table, String primaryKey) {
        return query(dataSourceConfig, String.format("select count(1), ifnull(min(%s),0), ifnull(max(%s),0) from %s", primaryKey, primaryKey, table)).stream().map(rows -> new TablePageInfo(Integer.parseInt(rows.get(0).getValue()), Long.parseLong(rows.get(1).getValue()), Long.parseLong(rows.get(2).getValue()))).findFirst().get();
    }

    /**
     * 查询主键
     *
     * @param dataSourceConfig 数据源配置
     * @return <主键，是否为数字类型>
     */
    private Optional<PriColInfo> queryPriKeys(SyncDataSourceConfig dataSourceConfig) {
        SyncDataSourceConfig newSource = dataSourceConfig.copy();
        newSource.setDatabase("information_schema");
        newSource.setTable("COLUMNS");
        return query(newSource, String.format("select TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_COMMENT from `COLUMNS` where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' and COLUMN_KEY = 'PRI'", dataSourceConfig.getDatabase(), dataSourceConfig.getTable()))
                .stream().map(rows -> {
                    return new PriColInfo(rows.get(0).getValue(), rows.get(1).getValue(), rows.get(2).getValue(), rows.get(3).getValue(), rows.get(4).getValue());
                }).findFirst();
    }

    /**
     * 更改已初始化状态
     *
     * @param loadStatus 状态
     */
    public void updateInitialized(LoadStatus loadStatus) {
        if (TYPE_ES.equals(loadStatus.getTarget())) {
            esMappingDao.updateInitialized(loadStatus);
        } else if (TYPE_REDIS.equals(loadStatus.getTarget())) {
            redisMappingDao.updateInitialized(loadStatus);
        } else if (TYPE_MQ.equals(loadStatus.getTarget())) {
            mqMappingDao.updateInitialized(loadStatus);
        } else if (TYPE_MYSQL.equals(loadStatus.getTarget())) {
            mysqlMappingDao.updateInitialized(loadStatus);
        } else {
            Log.error("不支持目标类型：{}", loadStatus.getTarget());
        }
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
        try {
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
        } catch (Exception e) {
            Log.warn("读取列值异常！type：{}，i：{}，errmsg：{}", type, i, e.getMessage());
            return null;
        }
    }

    /**
     * 删除主键自增
     *
     * @param mappingId  配置ID
     * @param priColInfo 主键信息
     */
    private void removeAutoIncrement(String mappingId, PriColInfo priColInfo) {
        MySQLSyncConfig mysqlSyncConfig = mysqlMappingDao.findById(mappingId);
        mysqlSyncConfig.getTargetDataSource().values().forEach(target -> {
            Connection conn = null;
            PreparedStatement preparedStatement = null;
            try {
                conn = jdbcConf.getConn(target);
                preparedStatement = conn.prepareStatement(String.format("alter table %s modify column %s %s comment '%s'", target.getTable(), priColInfo.getCol(), priColInfo.getType(), priColInfo.getComment()));
                preparedStatement.execute();
            } catch (SQLException e) {
                Log.errorThrow("删除主键自增失败！", e);
            } finally {
                try {
                    if (preparedStatement != null && !preparedStatement.isClosed()) {
                        preparedStatement.close();
                    }
                    if (conn != null && !conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    Log.errorThrow("关闭连接时发生异常！", e);
                }
            }
        });

    }

    /**
     * 添加主键自增
     *
     * @param mappingId  配置ID
     * @param priColInfo 主键信息
     */
    private void addAutoIncrement(String mappingId, PriColInfo priColInfo) {
        MySQLSyncConfig mysqlSyncConfig = mysqlMappingDao.findById(mappingId);
        mysqlSyncConfig.getTargetDataSource().values().forEach(target -> {
            Connection conn = null;
            PreparedStatement preparedStatement = null;
            try {
                conn = jdbcConf.getConn(target);
                preparedStatement = conn.prepareStatement(String.format("alter table %s modify column %s %s auto_increment comment '%s'", target.getTable(), priColInfo.getCol(), priColInfo.getType(), priColInfo.getComment()));
                preparedStatement.execute();
            } catch (SQLException e) {
                Log.errorThrow("添加主键自增失败！", e);
            } finally {
                try {
                    if (preparedStatement != null && !preparedStatement.isClosed()) {
                        preparedStatement.close();
                    }
                    if (conn != null && !conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    Log.errorThrow("关闭连接时发生异常！", e);
                }
            }
        });

    }


}
