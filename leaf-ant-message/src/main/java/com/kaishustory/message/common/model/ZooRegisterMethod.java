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

package com.kaishustory.message.common.model;

import lombok.Data;

import java.util.Objects;

/**
 * Zookeeper节点注册
 *
 * @author liguoyang
 * @create 2019-08-08 12:05
 **/
@Data
public class ZooRegisterMethod {

    /**
     * Zookeeper地址
     */
    private String zooAddr;
    /**
     * 方法
     */
    private String method;
    /**
     * 集群分组
     */
    private String group;
    /**
     * 消息主题
     */
    private String topic;
    /**
     * 服务地址
     */
    private String host;

    public ZooRegisterMethod() {
    }

    public ZooRegisterMethod(String zooAddr, String method, String group, String topic, String host) {
        this.zooAddr = zooAddr;
        this.method = method;
        this.group = group;
        this.topic = topic;
        this.host = host;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZooRegisterMethod that = (ZooRegisterMethod) o;
        return Objects.equals(zooAddr, that.zooAddr) &&
                Objects.equals(method, that.method) &&
                Objects.equals(group, that.group) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zooAddr, method, group, topic, host);
    }
}
