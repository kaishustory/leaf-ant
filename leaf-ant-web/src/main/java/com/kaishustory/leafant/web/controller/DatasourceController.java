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

import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.common.utils.Page;
import com.kaishustory.leafant.web.model.Datasource;
import com.kaishustory.leafant.web.service.DatasourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 数据源接口
 *
 * @author liguoyang
 * @create 2019-08-13 15:57
 **/
@RestController
@RequestMapping("/datasource")
public class DatasourceController {

    /**
     * 数据源Service
     */
    @Autowired
    private DatasourceService datasourceService;

    /**
     * 查询数据源列表
     */
    @GetMapping("/list")
    public Page<Datasource> list() {
        return Page.of(datasourceService.findDatabaseList(), 0, 0, 0);
    }

    /**
     * 查询数据源
     *
     * @param id 数据源ID
     * @return 数据源信息
     */
    @GetMapping("/get")
    public Option<Datasource> get(String id) {
        return datasourceService.getDatabase(id);
    }

    /**
     * 保存数据源
     *
     * @param datasource 数据源配置
     * @return 是否成功
     */
    @PostMapping("/save")
    public Option<String> save(@RequestBody Datasource datasource) {
        return datasourceService.saveDatabase(datasource);
    }

    /**
     * 删除数据源
     *
     * @param id 数据源ID
     * @return 是否成功
     */
    @GetMapping("/delete")
    public Option<String> delete(String id) {
        return datasourceService.deleteDatabase(id);
    }

    /**
     * 查询数据库列表
     *
     * @param id 数据源ID
     * @return 数据库列表
     */
    @GetMapping("/tables")
    public Option<List<String>> tables(String id) {
        return datasourceService.getTables(id);
    }

}
