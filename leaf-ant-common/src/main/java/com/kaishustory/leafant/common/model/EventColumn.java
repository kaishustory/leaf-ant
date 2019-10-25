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
 * 事件列
 **/
@Data
public class EventColumn {

    /**
     * 是否主键
     */
    private boolean isKey;

    /**
     * 列次序
     */
    private int index;

    /**
     * 列名
     */
    private String name;

    /**
     * 列值
     */
    private String value;

    /**
     * 列类型
     */
    private String mysqlType;

    /**
     * 列类型编号
     */
    private int sqlType;

    /**
     * 是否变更
     */
    private boolean updated;

    /**
     * 是否为空
     */
    private boolean isNull;

    public EventColumn() {
    }

    public EventColumn(boolean isKey, int index, String name, String value, String mysqlType, int sqlType, boolean updated, boolean isNull) {
        this.isKey = isKey;
        this.index = index;
        this.name = name;
        this.value = value;
        this.mysqlType = mysqlType;
        this.sqlType = sqlType;
        this.updated = updated;
        this.isNull = isNull;
    }
}
