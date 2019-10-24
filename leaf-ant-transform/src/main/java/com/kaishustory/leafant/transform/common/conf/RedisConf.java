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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis配置
 */
@Component
public class RedisConf {

    @Value("${redis-pool.max-idle}")
    int maxIdle;
    @Value("${redis-pool.min-idle}")
    int minIdle;
    @Value("${redis-pool.max-active}")
    int maxActive;
    @Value("${redis-pool.max-waits:3000}")
    long maxWaitMillis;
    @Value("${redis-timeout:5000}")
    int timeout;

    private Map<String, StringRedisTemplate> factoryPool = new HashMap<>();

    /**
     * 获得Redis连接
     *
     * @param addr     Redis地址（IP:端口）
     * @param password 密码
     * @param database 数据库
     * @return 连接
     */
    public StringRedisTemplate getConnection(String addr, String password, int database) {
        if (!factoryPool.containsKey(addr)) {
            synchronized (this) {
                if (!factoryPool.containsKey(addr)) {
                    StringRedisTemplate temple = new StringRedisTemplate();
                    String[] addrs = addr.split(":");
                    String host = addrs[0];
                    int port = addrs.length > 1 ? Integer.parseInt(addrs[1]) : 6379;
                    temple.setConnectionFactory(connectionFactory(host, port, password, database,
                            maxIdle, minIdle, maxActive, maxWaitMillis, timeout));
                    temple.afterPropertiesSet();
                    factoryPool.put(addr, temple);
                }
            }
        }
        return factoryPool.get(addr);
    }

    private RedisConnectionFactory connectionFactory(String hostName, int port,
                                                     String password, int database, int maxIdle, int minIdle, int maxActive,
                                                     long maxWaitMillis, int timeout) {
        JedisConnectionFactory jedis = new JedisConnectionFactory();
        jedis.setDatabase(database);
        jedis.setHostName(hostName);
        jedis.setPort(port);
        jedis.setTimeout(timeout);
        if (!StringUtils.isEmpty(password)) {
            jedis.setPassword(password);
        }
        jedis.setPoolConfig(poolConfig(maxIdle, minIdle, maxActive, maxWaitMillis, true));
        jedis.afterPropertiesSet();
        return jedis;
    }

    private JedisPoolConfig poolConfig(int maxIdle, int minIdle, int maxActive,
                                       long maxWaitMillis, boolean testOnBorrow) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setMaxTotal(maxActive);
        poolConfig.setMaxWaitMillis(maxWaitMillis);
        poolConfig.setTestOnBorrow(testOnBorrow);
        return poolConfig;
    }
}
