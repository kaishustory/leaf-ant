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

package com.kaishustory.message.producer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.kaishustory.message.common.model.RpcResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;


/**
 * 回调服务
 *
 * @author liguoyang
 * @create 2019-07-29 14:06
 **/
@Slf4j
public class CallbackServer {

    /**
     * 回调
     */
    private Cache<String, MessageCallback> callbackCache = Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

    /**
     * 注册回调事件
     *
     * @param msgId    消息ID
     * @param callback 回调事件
     */
    public void register(String msgId, MessageCallback callback) {
        callbackCache.put(msgId, callback);
    }

    /**
     * 回复事件
     *
     * @param msgId    消息ID
     * @param response 回复内容
     */
    public void reply(String msgId, RpcResponse response) {
        if (msgId == null) {
            throw new RuntimeException("reply message id is null");
        }
        // 获得回调事件
        MessageCallback callback = callbackCache.getIfPresent(msgId);
        if (callback != null) {
            // 回调消息
            callback.callback(response);
            // 删除回调处理
            callbackCache.invalidate(msgId);
        }
    }
}
