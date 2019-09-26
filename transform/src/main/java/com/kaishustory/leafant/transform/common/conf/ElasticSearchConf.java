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

package com.kaishustory.leafant.transform.common.conf;

import com.google.gson.GsonBuilder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ElasticSearch配置
 *
 * @author liguoyang
 * @create 2019-07-25 13:36
 **/
@Component
public class ElasticSearchConf {

    /**
     * 最大连接数
     */
    @Value("${es.conn.all-total}")
    private int connAllTotal;

    /**
     * 每个节点连接数
     */
    @Value("${es.conn.node-total}")
    private int connNodeTotal;

    private Map<String, JestClientFactory> factoryPool = new HashMap<>();

    /**
     * 创建 连接
     * @return
     */
    public JestClient getTransportClient(String esAddr) {
        if(!factoryPool.containsKey(esAddr)) {
            synchronized (this) {
                if(!factoryPool.containsKey(esAddr)) {
                    JestClientFactory factory = new JestClientFactory();
                    factory.setHttpClientConfig(new HttpClientConfig
                            // 服务器列表
                            .Builder(Arrays.stream(esAddr.split(",")).map(node -> "http://" + node).collect(Collectors.toList()))
                            .multiThreaded(true)
                            // 一个route 默认不超过2个连接  路由是指连接到某个远程主机的个数。总连接数 = route个数 * defaultMaxTotalConnectionPerRoute
                            .defaultMaxTotalConnectionPerRoute(connNodeTotal)
                            // 所有route连接总数
                            .maxTotalConnection(connAllTotal)
                            // 日期格式
                            .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create())
                            .build());
                    factoryPool.put(esAddr, factory);
                }
            }
        }
        return factoryPool.get(esAddr).getObject();
    }


}
