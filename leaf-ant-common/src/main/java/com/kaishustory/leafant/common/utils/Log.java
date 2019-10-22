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

package com.kaishustory.leafant.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志工具
 *
 * @author liguoyang
 * @create 2018-07-31 下午1:16
 **/
public class Log {

    /**
     * 获得类名称
     * @param level 调用栈级别
     * @return 类名称
     */
    private static String getClassName(Integer level){
        StackTraceElement stackTraceElement = new Throwable().getStackTrace()[level];
        return stackTraceElement.toString();
    }

    /**
     * 获得日志对象
     * @return 日志对象
     */
    private static Logger getLog(){
        return LoggerFactory.getLogger(getClassName(3));
    }

    /**
     * DEBUG日志
     * @param msg 日志信息
     */
    public static void debug(String msg) {
        getLog().debug(msg);
    }

    /**
     * DEBUG日志
     * @param msg 日志信息
     * @param var 参数信息
     */
    public static void debug(String msg,Object ... var) {
        getLog().debug(msg, var);
    }

    /**
     * INFO日志
     * @param msg 日志信息
     */
    public static void info(String msg) {
        getLog().info(msg);
    }

    /**
     * WARN日志
     * @param msg 日志信息
     * @param var 参数信息
     */
    public static void warn(String msg,Object ... var) {
        getLog().warn(msg, var);
    }

    /**
     * WARN日志
     * @param msg 日志信息
     */
    public static void warn(String msg) {
        getLog().warn(msg);
    }

    /**
     * INFO日志
     * @param msg 日志信息
     * @param var 参数信息
     */
    public static void info(String msg,Object ... var) {
        getLog().info(msg, var);
    }

    /**
     * ERROR日志
     * @param msg 日志信息
     * @param e 异常信息
     */
    public static void error(String msg, Throwable e){
        getLog().error(msg, e);
    }

    /**
     * ERROR日志
     * @param msg 日志信息
     */
    public static void error(String msg){
        getLog().error(msg);
    }

    /**
     * ERROR日志
     * @param msg 日志信息
     * @param var 参数信息
     */
    public static void error(String msg, Object ... var){
        getLog().error(msg,var);
    }

    /**
     * ERROR日志（并抛出异常）
     * @param msg 日志信息
     * @param e 异常信息
     */
    public static void errorThrow(String msg, Throwable e) throws RuntimeException{
        getLog().error(msg, e);
        throw new RuntimeException(msg, e);
    }

    /**
     * ERROR日志（并抛出异常）
     * @param msg 日志信息
     */
    public static void errorThrow(String msg) throws RuntimeException{
        getLog().error(msg);
        throw new RuntimeException(msg);
    }

    /**
     * ERROR日志（并抛出异常）
     * @param msg 日志信息
     * @param var 参数信息
     */
    public static void errorThrow(String msg, Object ... var) throws RuntimeException{
        getLog().error(msg,var);
        for(Object v : var){
            msg = msg.replaceFirst("\\{}", v.toString());
        }
        throw new RuntimeException(msg);
    }
}
