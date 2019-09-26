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

package com.kaishustory.web.service;

import com.kaishustory.web.dao.UserDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;

/**
 * 用户管理
 *
 * @author liguoyang
 * @create 2019-08-01 17:14
 **/
@Service
public class UserService {

    /**
     * 用户Dao
     */
    @Autowired
    private UserDao userDao;

    /**
     * 用户登录验证
     * @param user 用户名
     * @param password 密码
     * @return 是否验证通过
     */
    public boolean login(String user, String password){
        return userDao.findUser(user, DigestUtils.md5DigestAsHex(password.getBytes()))!=null;
    }

    /**
     * 注册用户
     * @param user 用户名
     * @param password 密码
     */
    public void addUser(String user, String password){
        userDao.addUser(user, DigestUtils.md5DigestAsHex(password.getBytes()));
    }
}
