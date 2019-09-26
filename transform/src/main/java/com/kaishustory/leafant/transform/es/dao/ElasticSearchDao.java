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

package com.kaishustory.leafant.transform.es.dao;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Time;
import com.kaishustory.leafant.transform.common.conf.ElasticSearchConf;
import com.kaishustory.leafant.transform.es.model.ChildQueryInfo;
import com.kaishustory.leafant.transform.es.model.EsMapping;
import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.MultiGet;
import io.searchbox.core.Search;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ElasticSearch工具
 *
 * @author liguoyang
 * @create 2019-07-26 10:45
 **/
@Component
public class ElasticSearchDao {

    /**
     * ElasticSearch连接
     */
    @Autowired
    private ElasticSearchConf elasticSearchConf;

    /**
     * 命令执行（返回）
     * @param esAddr ES地址
     * @param exec 命令
     * @param <T>
     * @return 执行结果
     */
    public <T> T execr(String esAddr,ExecReturn exec){
        JestClient client = elasticSearchConf.getTransportClient(esAddr);
        try {
            return (T) exec.handle(client);
        }catch (Exception e){
            Log.error("ES执行发生异常！", e);
            return null;
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                Log.error("关闭ES连接发生异常！", e);
            }
        }
    }

    /**
     * 命令执行（无返回）
     * @param esAddr ES地址
     * @param exec 命令
     */
    public void exec(String esAddr,Exec exec){
        JestClient client = elasticSearchConf.getTransportClient(esAddr);
        try {
            exec.handle(client);
        }catch (Exception e){
            Log.error("ES执行发生异常！", e);
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                Log.error("关闭ES连接发生异常！", e);
            }
        }
    }

    /**
     * 执行ES命令
     * @param esAddr ES地址
     * @param index 索引
     * @param type 类型
     * @param action ES动作
     */
    public boolean execr(String esAddr, String index, String type, Action action){
        JestClient client = elasticSearchConf.getTransportClient(esAddr);
        try {
            JestResult result = client.execute(action);
            if(result.isSucceeded()){
                Log.info("ES处理成功！es：{}，index：{}，type：{}", esAddr, index, type);
                return true;
            }else {
                Log.error("ES处理失败！es：{}，index：{}，type：{}，error：{}", esAddr, index, type, result.getJsonString());
                return false;
            }
        }catch (Exception e){
            Log.error("ES执行发生异常！", e);
            return false;
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                Log.error("关闭ES连接发生异常！", e);
            }
        }
    }

    /**
     * 批量命令
     * @param index 索引
     * @param type 类型
     * @param actionList 命令列表
     */
    public boolean bulk(String esAddr, String index, String type, List<BulkableAction> actionList){
        if(actionList.size()>0){
            return execr(esAddr, client -> {
                Time time = new Time("【ES】ES批处理命令");
                Bulk bulk = new Bulk.Builder()
                        .defaultIndex(index)
                        .defaultType(type)
                        .addAction(actionList)
                        .build();
                BulkResult result = client.execute(bulk);
                time.end();
                if(result.isSucceeded()){
                    Log.info("【ES】批处理成功！es：{}，index：{}，type：{}", esAddr, index, type);
                    return true;
                }else {
                    Log.errorThrow("【ES】批处理失败！es：{}，index：{}，type：{}，error：{}", esAddr, index, type, result.getJsonString());
                    return false;
                }
            });
        }else {
            return false;
        }
    }

    /**
     * 创建索引
     * @param index 索引
     * @param type 类型
     * @param esMapping 字段映射
     * @return 是否成功
     */
    public boolean createIndex(String esAddr,String index, String type, EsMapping esMapping){
        return execr(esAddr, client -> {
            // 创建索引
            Map<String, Object> settings = new HashMap<>(2);
            settings.put("number_of_shards", 5);
            settings.put("number_of_replicas", 1);
            JestResult indexResutl = client.execute(new CreateIndex.Builder(index).settings(settings).build());
            if(indexResutl.isSucceeded()){
                Log.info("ES创建索引成功！index：{}，type：{}", index, type);
            }else {
                Log.error("ES创建索引失败！index：{}，type：{}，error：{}", index, type, indexResutl.getErrorMessage());
            }

            // 创建索引映射
            JestResult mappingResult = client.execute(new PutMapping.Builder(
                    index,
                    type,
                    esMapping.toString()
            ).build());
            if(mappingResult.isSucceeded()){
                Log.info("ES创建索引映射成功！index：{}，type：{}", index, type);
            }else {
                Log.error("ES创建索引映射失败！index：{}，type：{}，error：{}", index, type, mappingResult.getErrorMessage());
            }
            return mappingResult.isSucceeded();
        });
    }

    /**
     * 查询
     * @param index 索引
     * @param queryFields 查询条件
     * @return
     */
    public List<JsonObject> query(String esAddr, String index, Map<String, Object> queryFields){
        return execr(esAddr, client -> {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.queryStringQuery(queryFields.entrySet().stream().map(field -> field.getKey()+": "+field.getValue()+"").reduce((a,b) -> a+" AND "+b).get()));
            Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(index).addType(index).build();
            JestResult rs = client.execute(search);
            return extractQueryResult(rs);
        });
    }

    /**
     * 批量查询子表数据
     * @param esAddr ES地址
     * @param index 索引
     * @param queryInfos 查询条件
     * @return 列表
     */
    public List<JsonObject> multChildQuery(String esAddr, String index, List<ChildQueryInfo> queryInfos){
        JestResult jestResult = execr(esAddr, client -> client.execute(new MultiGet.Builder.ById(index, index).addId(queryInfos.stream().map(ChildQueryInfo::getEsQueryId).collect(Collectors.toList())).build()));
        JsonArray actualDocs = jestResult.getJsonObject().getAsJsonArray("docs");
        List<JsonObject> docs = new ArrayList<>();
        actualDocs.forEach(doc -> {
            JsonObject source = doc.getAsJsonObject().getAsJsonObject("_source");
            docs.add(source);
        });
        return docs;
    }

    /**
     * 解析查询结果
     * @param rs
     * @return
     */
    private List<JsonObject> extractQueryResult(JestResult rs){
        List<JsonObject> docs = new ArrayList<>();
        rs.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits").forEach(doc -> {
            docs.add(extractResult(doc));
        });
        return docs;
    }

    private JsonObject extractResult(JsonElement doc){
        String id = doc.getAsJsonObject().get("_id").getAsString();
        JsonObject source = doc.getAsJsonObject().getAsJsonObject("_source");
        source.addProperty("_id", id);
        return source;
    }

    public interface ExecReturn{

        Object handle(JestClient client) throws IOException;
    }

    public interface Exec{

        void handle(JestClient client) throws IOException;
    }

}
