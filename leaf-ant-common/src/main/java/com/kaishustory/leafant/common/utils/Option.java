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

import java.io.Serializable;
import java.util.Optional;

/**
 * Service层返回类型
 *
 * @author liguoyang
 * @create 2018-08-01 下午4:43
 **/
public class Option<T> implements Serializable {

    /**
     * 值
     */
    private T value;
    /**
     * 错误码
     */
    private int errcode = 0;
    /**
     * 错误信息
     */
    private String errmsg;

    public Option(T value) {
        this.value = value;
    }

    /**
     * 返回空
     *
     * @param <T>
     * @return
     */
    public static <T> Option<T> empty() {
        return new Option(null);
    }

    /**
     * 返回错误
     *
     * @param errmsg 错误信息
     * @param <T>
     * @return
     */
    public static <T> Option<T> error(String errmsg) {
        return error(-1, errmsg);
    }

    /**
     * 返回错误
     *
     * @param errcode 错误码
     * @param errmsg  错误信息
     * @param <T>
     * @return
     */
    public static <T> Option<T> error(int errcode, String errmsg) {
        Option o = new Option(null);
        o.setErrcode(errcode);
        o.setErrmsg(errmsg);
        return o;
    }

    /**
     * 返回带值对象
     *
     * @param value 值
     * @param <T>
     * @return
     */
    public static <T> Option<T> of(T value) {
        return new Option(value);
    }

    /**
     * 返回带值对象
     *
     * @param optional 值
     * @param <T>
     * @return
     */
    public static <T> Option<T> of(Optional<T> optional) {
        if (optional.isPresent()) {
            return new Option(optional.get());
        } else {
            return Option.empty();
        }
    }

    /**
     * 是否有内容
     *
     * @return
     */
    public boolean exist() {
        return value != null;
    }

    /**
     * 是否无内容
     *
     * @return
     */
    public boolean nil() {
        return !exist();
    }

    /**
     * 是否错误
     *
     * @return
     */
    public boolean error() {
        return errcode != 0 || errmsg != null;
    }

    /**
     * 获得值
     *
     * @return
     */
    public T get() {
        return value;
    }

    /**
     * 返回值 或 返回默认值
     *
     * @param def 默认值
     * @return 返回值
     */
    public T getOr(T def) {
        if (exist()) {
            return value;
        } else {
            return def;
        }
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public int getErrcode() {
        return errcode;
    }

    public void setErrcode(int errcode) {
        this.errcode = errcode;
    }

    public String getErrmsg() {
        return errmsg;
    }

    public void setErrmsg(String errmsg) {
        this.errmsg = errmsg;
    }
}
