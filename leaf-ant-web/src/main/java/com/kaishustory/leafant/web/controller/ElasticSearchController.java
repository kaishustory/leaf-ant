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

package com.kaishustory.leafant.web.controller;

import com.kaishustory.leafant.common.model.EsSyncConfig;
import com.kaishustory.leafant.common.model.EsSyncMappingTable;
import com.kaishustory.leafant.common.model.SyncDataSourceConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.common.utils.Page;
import com.kaishustory.leafant.common.utils.Result;
import com.kaishustory.leafant.web.model.Datasource;
import com.kaishustory.leafant.web.service.DatasourceService;
import com.kaishustory.leafant.web.service.ElasticSearchMappingService;
import com.kaishustory.leafant.web.vo.ElasticSearchMappingVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * ElasticSearch映射Controller
 **/
@RestController
@RequestMapping("/elasticsearchMapping")
public class ElasticSearchController {

    /**
     * Es管理
     */
    @Autowired
    private ElasticSearchMappingService elasticSearchMappingService;

    /**
     * 数据源
     */
    @Autowired
    private DatasourceService datasourceService;

    /**
     * ES映射配置列表
     *
     * @param sourceTable 源表（查询条件）
     * @param page        页号
     * @param pageSize    每页条数
     * @return 列表
     */
    @GetMapping("/search")
    public Page<ElasticSearchMappingVo> search(@RequestParam(required = false) String sourceTable, @RequestParam(defaultValue = "1") int page, @RequestParam(defaultValue = "10") int pageSize) {
        return elasticSearchMappingService.search(sourceTable, page, pageSize);
    }

    /**
     * 创建索引
     *
     * @param esSyncConfig 索引定义
     * @return 返回结果
     */
    @PostMapping("/createIndex")
    public Result createIndex(@RequestBody EsSyncConfig esSyncConfig) {
        boolean success = elasticSearchMappingService.createIndex(esSyncConfig);
        if (success) {
            return new Result(Result.success, "success");
        } else {
            return new Result(Result.fail, "fail");
        }
    }

    /**
     * 快捷创建索引
     *
     * @param syncDataSourceConfig 数据源
     * @param esAddr               ES地址
     * @param esIndexManager       是否代管ES
     * @param indexName            自定义索引名称
     * @return 返回结果
     */
    @PostMapping("/fastCreateIndex")
    public Result fastCreateIndex(
            @RequestBody SyncDataSourceConfig syncDataSourceConfig,
            @RequestParam String esAddr,
            @RequestParam(defaultValue = "false") boolean esIndexManager,
            @RequestParam(required = false) String indexName
    ) {

        String index = indexName != null ? indexName : syncDataSourceConfig.getTable();
        // 简化单表索引
        EsSyncConfig esSyncConfig = new EsSyncConfig();
        esSyncConfig.setEsIndexManager(esIndexManager);
        esSyncConfig.setIndex(index);
        esSyncConfig.setType(index);
        esSyncConfig.setEsAddr(esAddr);
        esSyncConfig.setMult(false);

        EsSyncMappingTable mappingTable = elasticSearchMappingService.getTableMapping(syncDataSourceConfig);
        mappingTable.setMaster(true);
        esSyncConfig.setMasterTable(mappingTable);

        // 创建索引
        boolean success = elasticSearchMappingService.createIndex(esSyncConfig);
        if (success) {
            return new Result(Result.success, "success");
        } else {
            return new Result(Result.fail, "fail");
        }
    }

    /**
     * 初始化数据
     *
     * @param mappingId 数据同步定义ID
     * @return 返回结果
     */
    @GetMapping("/loadData")
    public Result loadData(String mappingId) {
        boolean success = elasticSearchMappingService.loadData(mappingId);
        if (success) {
            return new Result(Result.success, "success");
        } else {
            return new Result(Result.fail, "fail");
        }
    }

    /**
     * 修改同步状态
     *
     * @param mappingId  数据同步定义ID
     * @param syncStatus 是否同步
     * @return
     */
    @GetMapping("/syncStatus")
    public Result syncStatus(String mappingId, boolean syncStatus) {
        elasticSearchMappingService.updateSyncStatus(mappingId, syncStatus);
        return new Result(Result.success, "success");
    }

    /**
     * 查询表映射
     *
     * @param tables 表列表（[数据源ID.表名]）
     * @return 表结构映射
     */
    @GetMapping("/mapping")
    public Option<List<EsSyncMappingTable>> getEsMapping(String tables) {
        return Option.of(
                Arrays.stream(tables.split(",")).map(table -> table.split("\\.")).filter(tableInfo -> tableInfo.length == 2).map(tableInfo -> {

                    String databaseId = tableInfo[0];
                    String table = tableInfo[1];

                    // 查询数据库信息
                    Option<Datasource> database = datasourceService.getDatabase(databaseId);

                    if (database.nil() || database.error()) {
                        Log.error("数据源不存在！请确认配置信息。database：{}", databaseId);
                        return null;
                    }
                    // 查询表定义
                    return elasticSearchMappingService.getTableMapping(new SyncDataSourceConfig(database.get().getRds(), database.get().getUrl(), database.get().getDatabase(), table, database.get().getUsername(), database.get().getPassword()));
                }).collect(Collectors.toList())
        );
    }

}
