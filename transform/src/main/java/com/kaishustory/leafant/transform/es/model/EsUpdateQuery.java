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

import com.google.gson.JsonObject;
import com.kaishustory.leafant.common.utils.JsonUtils;
import lombok.Data;

import java.util.Map;

import static com.kaishustory.leafant.transform.es.model.EsUpdate.genScript;

/**
 * ElasticSearch更新结构
 *
 * @author liguoyang
 * @create 2019-07-26 16:56
 **/
@Data
public class EsUpdateQuery {

    private EsUpdate.Script script;

    private JsonObject query;

    public EsUpdateQuery(Map<String,Object> querys, Map<String,Object> fields) {
        this.script = new EsUpdate.Script(genScript(fields), fields);
        this.query = genQuery(querys);
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }

    /**
     * 查询脚本
     * @param querys 查询条件
     * @return 脚本
     */
    private JsonObject genQuery(Map<String,Object> querys){
        String queryString = querys.entrySet().stream().map(field -> field.getKey()+": "+field.getValue()+"").reduce((a,b) -> a+" AND "+b).get();
        return JsonUtils.toJsonObject("{\"query_string\": {\"query\": \"" + queryString +"\"}}");
    }

}
