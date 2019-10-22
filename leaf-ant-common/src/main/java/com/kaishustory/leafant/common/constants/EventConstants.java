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

package com.kaishustory.leafant.common.constants;

/**
 * 事件常量
 *
 * @author liguoyang
 * @create 2019-07-26 11:04
 **/
public class EventConstants {

    /**
     * 新增
     */
    public final static int TYPE_INSERT = 1;

    /**
     * 修改
     */
    public final static int TYPE_UPDATE = 2;

    /**
     * 删除
     */
    public final static int TYPE_DELETE = 3;

    /**
     * 事件动作：初始化加载
     */
    public final static String ACTION_LOAD = "load";

    /**
     * 事件动作：加载状态
     */
    public final static String ACTION_LOAD_STATUS = "load-status";

    /**
     * 事件动作：映射创建
     */
    public final static String ACTION_INDEX_CREATE = "create";

    /**
     * 事件动作：同步状态
     */
    public final static String ACTION_SYNC_STATUS = "sync-status";

    /**
     * 初始化加载状态：未初始化
     */
    public final static String LOAD_STATUS_NO = "no";

    /**
     * 初始化加载状态：初始化中
     */
    public final static String LOAD_STATUS_INITING = "initing";

    /**
     * 初始化加载状态：完成
     */
    public final static String LOAD_STATUS_COMPLETE = "complete";

    /**
     * 初始化加载状态：失败
     */
    public final static String LOAD_STATUS_FAIL = "fail";

    /**
     * 初始化加载状态：不支持初始化
     */
    public final static String LOAD_STATUS_NO_SUPPORT = "no-support";
}
