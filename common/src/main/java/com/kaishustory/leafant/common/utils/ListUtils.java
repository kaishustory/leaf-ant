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


import java.util.ArrayList;
import java.util.List;

/**
 * 集合处理工具
 *
 * @author liguoyang
 * @create 2019-01-12 1:25 AM
 **/
public class ListUtils {

    /**
     * 列表对齐合并
     * @param alist A列表
     * @param blist B列表
     * @param <A>
     * @param <B>
     * @return 合并列表
     */
    public static <A,B> List<Tuple2<A,B>> merge(List<A> alist, List<B> blist){
        List<Tuple2<A,B>> clist = new ArrayList<Tuple2<A,B>>();
        for (int i = 0; i < alist.size(); i++) {
            clist.add(new Tuple2<A,B>(alist.get(i), blist.get(i)));
        }
        return clist;
    }
}
