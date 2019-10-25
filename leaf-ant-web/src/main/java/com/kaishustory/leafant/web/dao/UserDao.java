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

package com.kaishustory.leafant.web.dao;

import com.kaishustory.leafant.web.model.User;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 用户Dao
 **/
@Component
public class UserDao {

    /**
     * 用户账号集合
     */
    private final String collection = "user";

    /**
     * Mongo
     */
    @Resource(name = "mongoTemplate")
    private MongoTemplate mongoTemplate;

    /**
     * 按用户名和密码查询用户
     *
     * @param user     用户名
     * @param password 密码（MD5）
     * @return 用户
     */
    public User findUser(String user, String password) {
        return mongoTemplate.findOne(Query.query(Criteria.where("user").is(user).and("password").is(password).and("open").is(true)), User.class, collection);
    }

    /**
     * 用户注册
     *
     * @param user     用户名
     * @param password 密码
     */
    public void addUser(String user, String password) {
        mongoTemplate.save(new User(user, password, true));
    }
}
