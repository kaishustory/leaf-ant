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

import lombok.Data;

/**
 * 页面返回类
 **/
@Data
public class Result {

    /**
     * 处理成功
     */
    public static int success = 0;
    /**
     * 处理失败
     */
    public static int fail = -1;
    // 错误码
    private Integer errcode = 0;
    // 错误信息
    private String errmsg;
    // 返回结果
    private Object result;

    /**
     * 返回错误信息
     *
     * @param errcode 错误码
     * @param errmsg  错误信息
     */
    public Result(Integer errcode, String errmsg) {
        this.errcode = errcode;
        this.errmsg = errmsg;
    }

    /**
     * 返回结果信息
     *
     * @param errcode 错误码
     * @param result  返回结果
     */
    public Result(Integer errcode, Object result) {
        this.errcode = errcode;
        this.result = result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
