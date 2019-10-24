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

package com.kaishustory.leafant.transform.es.model;

import com.kaishustory.leafant.common.utils.JsonUtils;
import lombok.Data;

import java.util.Map;

/**
 * ElasticSearch更新结构
 *
 * @author liguoyang
 * @create 2019-07-26 16:56
 **/
@Data
public class EsUpdate {

    private Script script;

    private Map<String, Object> upsert;

    public EsUpdate(Map<String, Object> fields) {
        this.script = new Script(genScript(fields), fields);
        this.upsert = fields;
    }

    /**
     * 更新脚本
     *
     * @param fields 更新字段
     * @return 脚本
     */
    protected static String genScript(Map<String, Object> fields) {
        return fields.entrySet().stream().map(field -> {
            if ("delete".equals(field.getValue())) {
                return "ctx._source.remove('" + field.getKey() + "')";
            } else {
                return "ctx._source." + field.getKey() + " = params." + field.getKey();
            }
        }).reduce((a, b) -> a + "; " + b).get();
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }

    @Data
    protected static class Script {
        private String source;
        private String lang = "painless";
        private Map<String, Object> params;

        public Script(String source, Map<String, Object> params) {
            this.source = source;
            this.params = params;
        }
    }
}
