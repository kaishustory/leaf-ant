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

package com.kaishustory.message.common.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/**
 * Zookeeper连接
 *
 * @author liguoyang
 * @create 2019-07-30 14:06
 **/
@Slf4j
public class ZooClient {

    /**
     * 连接
     */
    private static CuratorFramework client;

    /**
     * 获得Zookeeper连接
     *
     * @param addr Zookeeper地址
     * @return Zookeeper连接
     */
    public static synchronized CuratorFramework getClient(String addr) {
        if (client == null) {
            client = CuratorFrameworkFactory.builder()
                    .connectString(addr)
                    .sessionTimeoutMs(5000)
                    .connectionTimeoutMs(5000)
                    .retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
                    .build();
            client.getConnectionStateListenable().addListener((curatorFramework, connectionState) -> {
                log.info("Zookeeper状态变更：state：{}，isConnected：{}", connectionState.name(), connectionState.isConnected());
                // Zookeeper重连
                if ("RECONNECTED".equals(connectionState.name())) {
                    // 重新注册临时节点
                    ZooReconn.reconn();
                }
            });
            client.start();
        }
        return client;
    }

}
