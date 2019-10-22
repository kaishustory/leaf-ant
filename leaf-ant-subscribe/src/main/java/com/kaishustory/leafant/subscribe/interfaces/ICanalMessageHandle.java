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

package com.kaishustory.leafant.subscribe.interfaces;

import com.alibaba.otter.canal.protocol.Message;

/**
 * Canal订阅消息处理接口
 * @author liguoyang
 * @create 2018-05-06 下午1:46
 **/
public interface ICanalMessageHandle {

    /**
     * 订阅消息处理
     * @param message 数据变更消息
     * @return 是否处理成功
     */
    boolean handle(Message message);
}
