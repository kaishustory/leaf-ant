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

package com.kaishustory.leafant.subscribe.common.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.kaishustory.leafant.common.utils.StringUtils;
import com.kaishustory.leafant.common.utils.Time;
import com.kaishustory.leafant.subscribe.Application;
import com.kaishustory.leafant.subscribe.interfaces.ICanalMessageHandle;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

/**
 * Canal MySQL事件监听
 *
 * @author liguoyang
 * @create 2019-07-09 15:34
 **/
@Slf4j
public class CanalListener implements Runnable {

    /**
     * 数据库实例
     */
    private String server;

    /**
     * 消息处理
     */
    private ICanalMessageHandle canalMessageHandle;

    /**
     * 运行锁
     */
    private Lock runlock = new ReentrantLock();

    /**
     * 服务运行标识
     */
    private boolean running = true;

    /**
     * 构造
     *
     * @param server 数据库
     */
    public CanalListener(String server, ICanalMessageHandle canalMessageHandle) {
        this.server = server;
        this.canalMessageHandle = canalMessageHandle;
    }


    /**
     * Canal数据订阅
     */
    @Override
    @SneakyThrows
    public void run() {
        try {
            runlock.lock();
            while (running) {
                try {
                    // 创建 canal连接
                    CanalConnector conn = getConnection();
                    // 连接 canal
                    conn.connect();
                    // 订阅数据变更
                    conn.subscribe();

                    while (running) {
                        try {
                            // 读取数据变更消息
                            Message message = conn.getWithoutAck(1000);
                            // 批处理ID
                            long batchId = message.getId();
                            // 变更数量（只统计ROWDATA记录）
                            long size = message.getEntries().stream().filter(entry -> entry.getEntryType() == CanalEntry.EntryType.ROWDATA).count();

                            //判断是否有可处理消息
                            if (batchId == -1 || size == 0) {
                                // 回应处理成功
                                if (batchId != -1) {
                                    conn.ack(batchId);
                                }
                                //无更新消息，
//                                Thread.sleep(3);
                            } else {
                                Time time = new Time(format("任务处理. 数据库实例：%s，数据表：%s，BatchId：%d，Size：%d",
                                        server,
                                        message.getEntries().stream().filter(entry -> entry.getEntryType() == CanalEntry.EntryType.ROWDATA).map(entry -> entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName()).distinct().filter(StringUtils::isNotNull).reduce((a, b) -> a + "," + b).orElse(""),
                                        batchId,
                                        message.getEntries().stream().filter(entry -> entry.getEntryType() == CanalEntry.EntryType.ROWDATA).count()
                                ));
                                try {
                                    // 任务批量处理
                                    boolean handleResult = canalMessageHandle.handle(message);
                                    if (handleResult) {
                                        //确认处理成功
                                        conn.ack(batchId);
                                        log.info("任务处理成功！数据库实例：{}，BatchId：{}", server, batchId);
                                    } else {
                                        //处理失败，回滚数据
                                        conn.rollback(batchId);
                                        log.error("任务处理失败！数据库实例：{}，BatchId：{}", server, batchId);
                                    }
                                } catch (Throwable t) {
                                    //处理失败，回滚数据
                                    conn.rollback(batchId);
                                    log.error("任务处理发生异常！数据库实例：{}，BatchId：{}", server, batchId, t);
                                } finally {
                                    time.end();
                                }
                            }
                        } catch (Exception e) {
                            log.error("canal 消息订阅异常！数据库实例：{}", server, e);
                            //尝试重连
                            try {
                                conn.disconnect();
                                Thread.sleep(500);
                                conn.connect();
                                log.info("canal 重连！数据库实例：{}", server);
                            } catch (Exception e2) {
                                log.error("canal 重连失败！数据库实例：{}", server, e2);
                            }
                        }
                    }

                } catch (Exception e) {
                    log.error("canal 连接失败！数据库实例：{}", server, e);
                    Thread.sleep(1000);
                }
            }
        } finally {
            log.info("订阅数据已处理完成，可以关闭服务。数据库实例：{}", server);
            runlock.unlock();
        }

    }

    /**
     * 停止订阅
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        // 尝试获得运行锁，以等待已订阅数据处理完成
        runlock.lock();
        runlock.unlock();
    }

    /**
     * 获得Canal连接
     */
    private CanalConnector getConnection() {
        ConfigurableApplicationContext ctx = Application.getConfig();
        return CanalConnectors.newClusterConnector(
                ctx.getEnvironment().getProperty("zookeeper.url"),
                server,
                ctx.getEnvironment().getProperty("canal.user"),
                ctx.getEnvironment().getProperty("canal.password")
        );
    }
}
