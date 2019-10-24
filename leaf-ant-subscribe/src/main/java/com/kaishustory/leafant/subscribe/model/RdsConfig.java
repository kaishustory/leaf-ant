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

package com.kaishustory.leafant.subscribe.model;

import lombok.Data;

import java.util.List;

/**
 * RDS 配置信息
 *
 * @author liguoyang
 * @create 2019-07-16 17:31
 **/
@Data
public class RdsConfig {

    /**
     * 数据库
     */
    private String database;

    /**
     * 数据库服务实例
     */
    private List<String> serverList;

    /**
     * 用户名
     */
    private String user;

    /**
     * 密码
     */
    private String password;

    public RdsConfig(String database, List<String> serverList, String user, String password) {
        this.database = database;
        this.serverList = serverList;
        this.user = user;
        this.password = password;
    }
}
