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

import java.io.Serializable;
import java.util.List;

/**
 * 分页返回结果
 *
 * @author liguoyang
 * @create 2019-01-14 3:00 PM
 **/
@Data
public class Page<T> implements Serializable {

    /**
     * 记录总数
     */
    private int totalCount;

    /**
     * 总页数
     */
    private int totalPage;

    /**
     * 当前页号
     */
    private int currPage;

    /**
     * 每页条数
     */
    private int pageSize;

    /**
     * 错误码
     */
    private int errcode = 0;

    /**
     * 错误信息
     */
    private String errmsg;

    /**
     * 返回结果
     */
    private List<T> result;

    private Page(List<T> result, int totalCount, int currPage, int pageSize) {
        this.result = result;
        this.totalCount = totalCount;
        this.currPage = currPage;
        this.pageSize = pageSize;
        this.totalPage = (int) Math.ceil(totalCount / (float) pageSize);
    }

    private Page(int errcode, String errmsg) {
        this.errcode = errcode;
        this.errmsg = errmsg;
    }

    /**
     * 分页结果
     *
     * @param result     列表内容
     * @param totalCount 总条数
     * @param currPage   当前页号
     * @param pageSize   每页条数
     * @return 分页结果
     */
    public static <T> Page<T> of(List<T> result, int totalCount, int currPage, int pageSize) {
        return new Page<T>(result, totalCount, currPage, pageSize);
    }

    /**
     * 创建异常对象
     *
     * @param errcode 异常代码
     * @param errmsg  异常说明
     * @return 异常返回结果
     */
    public static <T> Page<T> error(int errcode, String errmsg) {
        return new Page<T>(errcode, errmsg);
    }

    /**
     * 创建异常对象
     *
     * @param errmsg 异常说明
     * @return 异常返回结果
     */
    public static <T> Page<T> error(String errmsg) {
        return new Page<T>(-1, errmsg);
    }

    /**
     * 是否有内容
     */
    public boolean exist() {
        return result != null;
    }

    /**
     * 是否无内容
     */
    public boolean nil() {
        return !exist();
    }

    /**
     * 是否错误
     */
    public boolean error() {
        return errcode != 0 || errmsg != null;
    }
}
