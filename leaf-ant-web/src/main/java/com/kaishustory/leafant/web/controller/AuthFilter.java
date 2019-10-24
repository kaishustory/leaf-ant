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

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 登录授权拦截
 *
 * @author liguoyang
 * @create 2019-08-01 14:35
 **/
@Slf4j
@Component
@WebFilter(urlPatterns = {"/**"})
public class AuthFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) {

    }

    /**
     * 用户权限拦截检查
     *
     * @param servletRequest  请求
     * @param servletResponse 返回
     * @param filterChain     过滤
     */
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        try {
            // 跳过验证
            if (
                // 登录页面
                    request.getRequestURI().equals("/page/login")
                            // 登录请求
                            || request.getRequestURI().equals("/login")
                            // 静态资源
                            || request.getRequestURI().startsWith("/static")
//                // 测试请求
//                || request.getRequestURI().startsWith("/elasticsearchMapping")
//                || request.getRequestURI().startsWith("/redisMapping")
//                || request.getRequestURI().startsWith("/mqMapping")
//                || request.getRequestURI().startsWith("/mysqlMapping")
                    ) {
                // 允许访问
                filterChain.doFilter(request, response);
                return;
            }

            // 读取用户token
            Object token = request.getSession().getAttribute("token");
            // 检查是否有token
            if (token != null) {
                // 允许访问
                filterChain.doFilter(request, response);
                return;
            } else {
                log.info("用户未登录，前往登录页！uri：{}", request.getRequestURI());
                // 调至登录页面
                response.sendRedirect("/page/login");
                return;
            }
        } catch (Exception e) {
            log.error("登录验证异常！uri：{}", request.getRequestURI(), e);
        }
    }

    @Override
    public void destroy() {

    }
}
