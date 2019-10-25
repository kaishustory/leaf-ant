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

package com.kaishustory.message.common.model;

import lombok.Data;

/**
 * 回复参数
 *
 * @author liguoyang
 * @create 2019-07-29 11:27
 **/
@Data
public class RpcResponse {

    /**
     * 不回复
     */
    public static RpcResponse NO_REPLY = new RpcResponse("NO_REPLY", null, 0);
    /**
     * 成功消息
     */
    public static int STATUS_SUCCESS = 1;
    /**
     * 失败失败
     */
    public static int STATUS_FAIL = -1;
    /**
     * 推送消息
     */
    public static int STATUS_PUSH = 2;
    /**
     * 消息ID
     */
    private String id;
    /**
     * 消息类型（动作类型）
     */
    private String action;
    /**
     * 消息内容
     */
    private String data;
    /**
     * 响应状态（1：成功，-1：失败，2：推送）
     */
    private int status;
    /**
     * 异常信息
     */
    private String errmsg;

    public RpcResponse() {
    }

    public RpcResponse(int status, String errmsg) {
        this.status = status;
        this.errmsg = errmsg;
    }

    public RpcResponse(String action, String data, int status) {
        this.action = action;
        this.data = data;
        this.status = status;
    }

    /**
     * 失败响应
     */
    public static RpcResponse errorResponse(String errmsg) {
        return new RpcResponse(STATUS_FAIL, errmsg);
    }

    /**
     * 是否成功
     */
    public boolean success() {
        return status == STATUS_SUCCESS;
    }

}
