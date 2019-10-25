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

import java.util.ArrayList;
import java.util.List;

/**
 * 请求响应集合
 **/
@Data
public class RpcResponseCollection {

    /**
     * 消费者数量
     */
    private int consumerSize;
    /**
     * 响应结果
     */
    private List<RpcResponse> responses = new ArrayList<>();
    /**
     * 异常信息
     */
    private String errmsg;

    public RpcResponseCollection(int consumerSize, List<RpcResponse> responses) {
        this.consumerSize = consumerSize;
        this.responses = responses;
    }

    public RpcResponseCollection(int consumerSize, List<RpcResponse> responses, String errmsg) {
        this.consumerSize = consumerSize;
        this.responses = responses;
        this.errmsg = errmsg;
    }

    /**
     * 是否全部成功
     */
    public boolean success() {
        return consumerSize == getSuccessSize();
    }


    /**
     * 成功数量
     */
    public int getSuccessSize() {
        return Long.valueOf(responses.stream().filter(RpcResponse::success).count()).intValue();
    }

    /**
     * 失败数量
     */
    public int getFailSize() {
        return Long.valueOf(responses.stream().filter(response -> !response.success()).count()).intValue();
    }
}
