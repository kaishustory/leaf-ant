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

package com.kaishustory.message.producer;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 发送状态
 **/
@Data
public class Stats {

    /**
     * 消息ID
     */
    private String msgId;
    /**
     * 分组
     */
    private String group;
    /**
     * 主题
     */
    private String topic;
    /**
     * 客户端状态列表
     */
    private List<ConsumerStats> consumerStats = new ArrayList<>();

    public Stats() {
    }

    public Stats(String msgId, String group, String topic, List<ConsumerStats> consumerStats) {
        this.msgId = msgId;
        this.group = group;
        this.topic = topic;
        this.consumerStats = consumerStats;
    }

    /**
     * 是否全部发送成功
     */
    public boolean success() {
        return consumerStats.stream().map(ConsumerStats::isSend).reduce((s1, s2) -> s1 && s2).orElse(false);
    }

    /**
     * 客户端状态
     */
    @Data
    public static class ConsumerStats {

        /**
         * 客户端地址
         */
        private String addr;
        /**
         * 是否发送成功
         */
        private boolean send;

        public ConsumerStats() {
        }

        public ConsumerStats(String addr, boolean send) {
            this.addr = addr;
            this.send = send;
        }

    }
}
