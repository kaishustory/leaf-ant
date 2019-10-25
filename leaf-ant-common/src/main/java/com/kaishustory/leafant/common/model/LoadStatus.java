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

package com.kaishustory.leafant.common.model;

import lombok.Data;

/**
 * 初始化状态
 **/
@Data
public class LoadStatus {

    /**
     * 同步目标（ElasticSearch：es，Redis：redis，MQ：mq）
     */
    private String target;

    /**
     * 映射ID
     */
    private String mappingId;

    /**
     * 初始化状态：no：未初始化，initing：初始化中，complete：完成，fail：失败
     */
    private String loadStatus;

    public LoadStatus() {
    }

    public LoadStatus(String target, String mappingId, String loadStatus) {
        this.target = target;
        this.mappingId = mappingId;
        this.loadStatus = loadStatus;
    }
}
