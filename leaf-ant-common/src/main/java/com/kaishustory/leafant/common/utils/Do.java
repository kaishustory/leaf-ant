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


/**
 * 处理工具类
 *
 * @author liguoyang
 * @create 2019-04-26 18:26
 **/
public class Do {

    public static <T> Option<T> task(String taskName, TaskHandle taskHandle){
        try {
            Log.info("【开始】"+taskName);
            Object result = taskHandle.handle();
            Log.info("【成功】"+taskName);
            return Option.of((T) result);
        }catch (Exception e){
            Log.error("【失败】"+taskName, e);
            return Option.error(e.getMessage());
        }
    }

    public static Option<Boolean> task(String taskName, TaskHandleNoResult taskHandle){
        try {
            Log.info("【开始】"+taskName);
            taskHandle.handle();
            Log.info("【成功】"+taskName);
            return Option.of(true);
        }catch (Exception e){
            Log.error("【失败】"+taskName, e);
            return Option.error(e.getMessage());
        }
    }

    public static <T> Option<T> task(String taskName, TaskHandleOption<T> taskHandle){
        try {
            Log.info("【开始】"+taskName);
            Option result = taskHandle.handle();
            if(!result.error()) {
                Log.info("【成功】" + taskName);
            }else {
                Log.error("【失败】"+taskName + " {}", result.getErrmsg());
            }
            return result;
        }catch (Exception e){
            Log.error("【失败】"+taskName, e);
            return Option.error(e.getMessage());
        }
    }

    public static <T> Page<T> task(String taskName, TaskHandlePage<T> taskHandle){
        try {
            Log.info("【开始】"+taskName);
            Page result = taskHandle.handle();
            if(result.exist()) {
                Log.info("【成功】" + taskName);
            }else {
                Log.error("【失败】"+taskName + " {}", result.getErrmsg());
            }
            return result;
        }catch (Exception e){
            Log.error("【失败】"+taskName, e);
            return Page.error(e.getMessage());
        }
    }

    public interface TaskHandle{
        Object handle();
    }
    public interface TaskHandleNoResult{
        void handle();
    }

    public interface TaskHandleOption<T>{
        Option<T> handle();
    }

    public interface TaskHandlePage<T>{
        Page<T> handle();
    }
}
