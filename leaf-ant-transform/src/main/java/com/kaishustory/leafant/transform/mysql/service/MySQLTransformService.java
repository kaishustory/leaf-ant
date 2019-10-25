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

package com.kaishustory.leafant.transform.mysql.service;

import com.kaishustory.leafant.transform.mysql.dao.MySQLDao;
import com.kaishustory.leafant.transform.mysql.model.MySQLEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

import static com.kaishustory.leafant.common.constants.EventConstants.TYPE_DELETE;

/**
 * MySQL数据转换处理
 **/
@Service
public class MySQLTransformService {

    /**
     * MySQL Dao
     */
    @Autowired
    private MySQLDao mysqlDao;

    /**
     * 事件转换处理
     *
     * @param eventList 事件列表
     */
    public void eventHandle(List<MySQLEvent> eventList) {

        // 按目标数据源分组
        eventList.stream().collect(Collectors.groupingBy(MySQLEvent::getTargetDataSource)).forEach((target, targetGroupEvent) -> {
            // 按操作类型分组
            targetGroupEvent.stream().collect(Collectors.groupingBy(e -> e.getEvent().getType() == TYPE_DELETE)).forEach((isDel, events) -> {
                if (!isDel) {
                    /** 新增、修改处理 **/
                    mysqlDao.batchUpdate(target, events);

                } else {
                    /** 删除处理 **/
                    mysqlDao.batchDelete(target, events);
                }
            });
        });
    }


}
