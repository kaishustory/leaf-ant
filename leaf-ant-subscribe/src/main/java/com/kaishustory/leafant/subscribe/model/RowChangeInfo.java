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

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;

/**
 * 数据变更信息
 *
 * @author liguoyang
 * @create 2018-05-06 下午5:42
 **/
@Data
public class RowChangeInfo {

    /**
     * 基本信息
     */
    private CanalEntry.Header header;
    /**
     * 变更信息
     */
    private CanalEntry.RowChange rowChange;

    public RowChangeInfo() {
    }

    public RowChangeInfo(CanalEntry.Header header, CanalEntry.RowChange rowChange) {
        this.header = header;
        this.rowChange = rowChange;
    }
}
