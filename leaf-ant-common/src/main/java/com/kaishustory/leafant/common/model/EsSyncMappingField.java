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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.sql.Types;

/**
 * ElasticSearch字段同步映射配置
 **/
@Data
public class EsSyncMappingField {

    /**
     * 字段名
     */
    private String field;

    /**
     * MySQL类型名称
     */
    private String typeName;

    /**
     * MySQL类型数值
     */
    private int type;

    /**
     * ES类型名称
     */
    private String esTypeName;

    /**
     * 是否索引
     */
    private boolean index;

    /**
     * 是否分词
     */
    private boolean analyzer;

    /**
     * 源列
     */
    private String sourceColumn;

    /**
     * 主键
     */
    private boolean primaryKey = false;

    /**
     * 关联外表【主表字段】
     */
    private boolean foreignKey = false;

    /**
     * 关联外表MySQL字段【主表字段】
     */
    private String joinChildColumn;

    /**
     * 被主表关联【子表字段】
     */
    private boolean joinKey = false;

    /**
     * 关联主表MySQL字段【子表字段】
     */
    private String joinMasterColumn;

    /**
     * 关联主表ES字段【子表字段】
     */
    private String joinMasterEsField;
    /**
     * 是否同步
     */
    private boolean sync = true;
    /**
     * 外键关联ES字段（外键必须为主表主键）
     */
    private String foreignField;

    public EsSyncMappingField() {
    }

    public EsSyncMappingField(String field, String typeName, int type, boolean index, boolean analyzer, String sourceColumn) {
        this.field = field;
        this.typeName = typeName;
        this.type = type;
        this.index = index;
        this.analyzer = analyzer;
        this.sourceColumn = sourceColumn;
        this.esTypeName = getEsType(type, analyzer);
    }


    public EsSyncMappingField(String field, String typeName, int type, boolean index, boolean analyzer, String sourceColumn, boolean primaryKey, boolean foreignKey, String foreignField) {
        this.field = field;
        this.typeName = typeName;
        this.type = type;
        this.index = index;
        this.analyzer = analyzer;
        this.sourceColumn = sourceColumn;
        this.primaryKey = primaryKey;
        this.foreignKey = foreignKey;
        this.foreignField = foreignField;
        this.esTypeName = getEsType(type, analyzer);
    }

    /**
     * 关联主表ES字段名称【子表字段】
     */
    @JsonIgnore
    public String getJoinMasterEsFieldName() {
        return joinMasterEsField.split("\\.")[1];
    }

    @JsonIgnore
    public String getEsTypeName() {
        if (esTypeName == null) {
            esTypeName = getEsType(type, analyzer);
        }
        return esTypeName;
    }

    /**
     * 转换Es类型
     *
     * @param type     列类型
     * @param analyzer 是否分词
     * @return Es类型
     */
    @JsonIgnore
    private String getEsType(int type, boolean analyzer) {
        switch (type) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIT:
                return "integer";
            case Types.BIGINT:
                return "long";
            case Types.FLOAT:
                return "float";
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return "double";
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return "date";
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            default:
                return analyzer ? "text" : "keyword";
        }
    }
}
