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

import com.kaishustory.leafant.common.model.RedisSyncConfig;
import com.kaishustory.leafant.common.utils.Page;
import com.kaishustory.leafant.common.utils.Result;
import com.kaishustory.leafant.web.service.RedisMappingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * Redis映射Controller
 **/
@RestController
@RequestMapping("/redisMapping")
public class RedisController {

    /**
     * Redis管理
     */
    @Autowired
    private RedisMappingService redisMappingService;

    /**
     * Redis映射配置列表
     *
     * @param sourceTable 源表（查询条件）
     * @param page        页号
     * @param pageSize    每页条数
     * @return 列表
     */
    @GetMapping("/search")
    public Page<RedisSyncConfig> search(@RequestParam(required = false) String sourceTable, @RequestParam(defaultValue = "1") int page, @RequestParam(defaultValue = "10") int pageSize) {
        return redisMappingService.search(sourceTable, page, pageSize);
    }

    /**
     * 创建映射
     *
     * @param redisSyncConfig 映射定义
     * @return 返回结果
     */
    @PostMapping("/createMapping")
    public Result createMapping(@RequestBody RedisSyncConfig redisSyncConfig) {
        boolean success = redisMappingService.createMapping(redisSyncConfig);
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
        boolean success = redisMappingService.loadData(mappingId);
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
        redisMappingService.updateSyncStatus(mappingId, syncStatus);
        return new Result(Result.success, "success");
    }

}
