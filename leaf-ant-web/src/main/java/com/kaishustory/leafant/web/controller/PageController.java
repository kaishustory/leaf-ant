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

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 页面控制
 **/
@Controller
@RequestMapping("/page")
public class PageController {

    /**
     * 登录页面
     */
    @RequestMapping("/login")
    public String login() {
        return "login";
    }

    /**
     * 首页页面
     */
    @RequestMapping("/index")
    public String index() {
        return "elasticsearch";
    }

    /**
     * ElasticSearch配置列表
     */
    @RequestMapping("/elasticsearch")
    public String elasticsearch() {
        return "elasticsearch";
    }

    /**
     * ElasticSearch数据源配置页面
     */
    @RequestMapping("/elasticsearchDataSource")
    public String elasticsearchDatasourceConfig() {
        return "elasticsearch_datasource";
    }

    /**
     * ElasticSearch映射配置页面
     */
    @RequestMapping("/elasticsearchMapping")
    public String elasticsearchMappingConfig() {
        return "elasticsearch_mapping";
    }
}
