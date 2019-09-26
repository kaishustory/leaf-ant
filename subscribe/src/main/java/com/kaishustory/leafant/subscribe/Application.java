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

package com.kaishustory.leafant.subscribe;

import com.kaishustory.leafant.subscribe.common.utils.BeanFactory;
import com.kaishustory.leafant.subscribe.common.canal.CanalListenRegisterService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.dao.PersistenceExceptionTranslationAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * 服务启动
 *
 * @author liguoyang
 * @create 2019-04-26 14:05
 **/
@SpringBootApplication(exclude = {PersistenceExceptionTranslationAutoConfiguration.class, DataSourceAutoConfiguration.class, MongoDataAutoConfiguration.class, MongoAutoConfiguration.class})
@ComponentScan(basePackages = {"com.kaishustory.leafant.mapping", "com.kaishustory.leafant.subscribe"})
public class Application {

    /**
     * 系统配置
     */
    private static ConfigurableApplicationContext ctx;

    /**
     * 项目启动
     * @param args
     */
    public static void main(String[] args) {
        // 启动服务
        ctx = SpringApplication.run(Application.class, args);
        // 订阅Canal消息
        BeanFactory.getBean(CanalListenRegisterService.class).canalListenRegister();
    }

    /**
     * 获取系统配置
     */
    public static ConfigurableApplicationContext getConfig(){
        return ctx;
    }

}
