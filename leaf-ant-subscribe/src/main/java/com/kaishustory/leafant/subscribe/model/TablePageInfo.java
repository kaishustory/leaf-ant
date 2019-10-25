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

package com.kaishustory.leafant.subscribe.model;

import lombok.Data;

/**
 * 表分页信息
 **/
@Data
public class TablePageInfo {

    /**
     * 总记录数
     */
    private int total;

    /**
     * 最小主键
     */
    private long minKey;

    /**
     * 最大主键
     */
    private long maxKey;

    public TablePageInfo(int total, long minKey, long maxKey) {
        this.total = total;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }
}
