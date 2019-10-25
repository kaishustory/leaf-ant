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

import com.kaishustory.leafant.common.utils.Result;
import com.kaishustory.leafant.web.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Base64;

/**
 * 用户登录
 **/
@Slf4j
@RestController
public class LoginController {

    /**
     * 用户管理
     */
    @Autowired
    private UserService userService;

    /**
     * 账号注册
     *
     * @param user     用户名
     * @param password 密码
     * @return
     */
    @GetMapping("/addUser")
    public Result addUser(String user, String password) {
        try {
            userService.addUser(user, password);
            return new Result(200, "注册成功");
        } catch (Exception e) {
            return new Result(500, "注册失败！");
        }
    }

    /**
     * 用户登录
     *
     * @param user     用户名
     * @param password 密码
     * @param request  请求
     * @return
     */
    @GetMapping("/login")
    public Result login(@RequestParam String user, @RequestParam String password, HttpServletRequest request) {
        // 用户登录验证
        if (userService.login(user, password)) {
            // 写入登录Token
            addSession(user, password, request);
            log.info("用户登录：User：{}", user);
            return new Result(200, "登录成功");
        } else {
            return new Result(500, "用户名或密码错误！");
        }
    }

    /**
     * 用户登出
     */
    @GetMapping("/logout")
    public Result logout(HttpServletRequest request) {
        clearSession(request);
        return new Result(200, "登出成功");
    }

    private String getToken(String user, String password) {
        return Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
    }

    /**
     * 写入Session
     *
     * @param user     用户名
     * @param password 密码
     * @param request  请求
     */
    private void addSession(String user, String password, HttpServletRequest request) {
        // 写入登录Token
        request.getSession().setAttribute("token", getToken(user, password));
        request.getSession().setAttribute("user", user);
    }

    /**
     * 清理Session
     *
     * @param request 请求
     */
    private void clearSession(HttpServletRequest request) {
        request.getSession().invalidate();
    }
}
