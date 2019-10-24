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

package com.kaishustory.leafant.web.service;

import com.kaishustory.leafant.common.model.SyncDataSourceConfig;
import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.common.utils.Option;
import com.kaishustory.leafant.web.dao.DatasourceDao;
import com.kaishustory.leafant.web.dao.TableDefineDao;
import com.kaishustory.leafant.web.model.Datasource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 数据源管理Service
 *
 * @author liguoyang
 * @create 2019-08-13 16:53
 **/
@Service
public class DatasourceService {

    /**
     * 数据源Dao
     */
    @Autowired
    private DatasourceDao datasourceDao;

    /**
     * 表定义Dao
     */
    @Autowired
    private TableDefineDao tableDefineDao;

    /**
     * 查询数据源列表
     */
    public List<Datasource> findDatabaseList() {
        return datasourceDao.findDatabaseList();
    }

    /**
     * 查询数据源
     *
     * @param id 数据源ID
     * @return 数据源信息
     */
    public Option<Datasource> getDatabase(String id) {
        try {
            return Option.of(datasourceDao.get(id));
        } catch (Exception e) {
            Log.error("查询数据源失败！id：{}", id, e);
            return Option.error(e.getMessage());
        }
    }

    /**
     * 保存数据源
     *
     * @param datasource 数据源信息
     */
    public Option<String> saveDatabase(Datasource datasource) {
        try {
            datasourceDao.save(datasource);
            Log.info("保存数据源成功。Datasource：{}", datasource);
            return Option.of("ok");
        } catch (Exception e) {
            Log.error("保存数据源失败！Datasource：{}", datasource, e);
            return Option.error(e.getMessage());
        }
    }

    /**
     * 删除数据源
     *
     * @param id 数据源ID
     */
    public Option<String> deleteDatabase(String id) {
        try {
            datasourceDao.delete(id);
            Log.info("删除数据源成功。id：{}", id);
            return Option.of("ok");
        } catch (Exception e) {
            Log.error("删除数据源失败！id：{}", id, e);
            return Option.error(e.getMessage());
        }
    }

    /**
     * 查询数据库列表
     *
     * @param id 数据源ID
     * @return 数据库列表
     */
    public Option<List<String>> getTables(String id) {
        Option<Datasource> database = getDatabase(id);
        if (database.exist()) {
            return Option.of(tableDefineDao.getTables(new SyncDataSourceConfig(database.get().getRds(), database.get().getUrl(), database.get().getDatabase(), "", database.get().getUsername(), database.get().getPassword())));
        } else {
            return Option.empty();
        }
    }
}
