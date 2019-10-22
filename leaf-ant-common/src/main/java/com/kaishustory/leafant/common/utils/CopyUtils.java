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
 * 对象克隆
 * @author liguoyang
 * @create 2018-11-30 12:14 PM
 **/
public class CopyUtils {

    /**
     * 对象克隆（深层）
     * @param src 原对象
     * @param cls 对象类型
     * @param <T> 类型
     * @return 新对象
     */
    public static<T> T clone(T src, Class<T> cls) {
        return JsonUtils.fromJson(JsonUtils.toJson(src), cls);
    }

}
