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

import com.kaishustory.leafant.common.model.MySQLSyncConfig;
import com.kaishustory.leafant.common.utils.Page;
import com.kaishustory.leafant.common.utils.Result;
import com.kaishustory.leafant.web.service.MySQLMappingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * MySQL映射Controller
 *
 * @author liguoyang
 * @create 2019-08-02 14:02
 **/
@RestController
@RequestMapping("/mysqlMapping")
public class MySQLController {

    /**
     * MySQL管理
     */
    @Autowired
    private MySQLMappingService mysqlMappingService;

    /**
     * MySQL映射配置列表
     * @param sourceTable 源表（查询条件）
     * @param page 页号
     * @param pageSize 每页条数
     * @return 列表
     */
    @GetMapping("/search")
    public Page<MySQLSyncConfig> search(@RequestParam(required = false) String sourceTable, @RequestParam(defaultValue = "1") int page, @RequestParam(defaultValue = "10") int pageSize){
        return mysqlMappingService.search(sourceTable, page, pageSize);
    }

    /**
     * 创建映射
     * @param mysqlSyncConfig 映射定义
     * @return 返回结果
     */
    @PostMapping("/createMapping")
    public Result createMapping(@RequestBody MySQLSyncConfig mysqlSyncConfig){
        boolean success = mysqlMappingService.createMapping(mysqlSyncConfig);
        if(success) {
            return new Result(Result.success, "success");
        }else {
            return new Result(Result.fail, "fail");
        }
    }

    /**
     * 初始化数据
     * @param mappingId 数据同步定义ID
     * @return 返回结果
     */
    @GetMapping("/loadData")
    public Result loadData(String mappingId){
        boolean success = mysqlMappingService.loadData(mappingId);
        if(success) {
            return new Result(Result.success, "success");
        }else {
            return new Result(Result.fail, "fail");
        }
    }

    /**
     * 修改同步状态
     * @param mappingId 数据同步定义ID
     * @param syncStatus 是否同步
     * @return
     */
    @GetMapping("/syncStatus")
    public Result syncStatus(String mappingId, boolean syncStatus){
        mysqlMappingService.updateSyncStatus(mappingId, syncStatus);
        return new Result(Result.success, "success");
    }

}
