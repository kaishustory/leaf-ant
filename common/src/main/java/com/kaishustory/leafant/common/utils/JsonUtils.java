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



import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Json转换工具
 *
 * @author liguoyang
 * @create 2019-03-08 11:35 AM
 **/
public class JsonUtils {

    /**
     * Gson
     */
    private static Gson gson;

    /**
     * Json 编译器
     */
    private static JsonParser jsonParser;

    private static Gson getGson(){
        if(gson==null){
            gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").disableHtmlEscaping().create();
        }
        return gson;
    }

    private static JsonParser getParser(){
        if(jsonParser==null){
            jsonParser = new JsonParser();
        }
        return jsonParser;
    }

    /**
     * JSON 转 对象
     * @param json JSON
     * @param cls 对象类型
     * @return 对象
     */
    public static <T> T fromJson(String json, Class<T> cls){
        return getGson().fromJson(json,cls);
    }

    /**
     * 对象 转 JSON
     * @param obj 对象
     * @return JSON
     */
    public static String toJson(Object obj){
        return getGson().toJson(obj);
    }

    /**
     * Json 转 Json对象
     * @param json JSON
     * @return Json对象
     */
    public static JsonObject toJsonObject(String json){
        return getParser().parse(json).getAsJsonObject();
    }

    /**
     * Json 转 JsonArray对象
     * @param json JSON
     * @return Json对象
     */
    public static JsonArray toJsonList(String json){
        return getParser().parse(json).getAsJsonArray();
    }

    /**
     * Json 转 数组
     * @param json JSON
     * @param cls 对象类型
     * @return 列表
     */
    public static <T> T toList(String json, Class<T> cls){
        return fromJson(json, cls);
    }

    /**
     * Map 转 对象
     * @param map Map<String, Any>? Map内容
     * @param beanClass Class<T> 类型定义
     */
    public static <T> T mapToObject(Map<String, Object> map, Class<T> beanClass) {
        try {
            if (map == null) {
                return null;
            }
            T obj = beanClass.newInstance();

            BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

            Arrays.stream(propertyDescriptors).forEach(property -> {
                if("class".equals(property.getName())){
                    return;
                }
                Method setter = property.getWriteMethod();
                if(setter!=null) {
                    Object value = map.get(property.getName());
                    // 日期类型转换
                    if (setter.getParameterTypes()[0].getTypeName().equals(Date.class.getName()) && value instanceof String) {
                        value = DateUtils.toTime(value.toString());
                    }
                    if (map.containsKey(property.getName())) {
                        try {
                            setter.invoke(obj, value);
                        } catch (Exception e) {
                            Log.error("Map to Object field set value error. field: {}, value: {}", property.getName(), map.get(property.getName()), e);
                        }
                    }
                }
            });

            return (T) obj;
        }catch (Exception e){
            Log.error("Map to Object 发生异常!", e);
            return null;
        }
    }

    /**
     * 对象 转 Map
     * @param obj Any? 对象
     * @return Map<String, Any?> Map
     */
    public static Map objectToMap(Object obj){
        try {
            if (obj == null) {
                return new HashMap(0);
            }

            Map<String, Object> map = new HashMap<>();

            BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            Arrays.stream(propertyDescriptors).forEach(property -> {
                try {
                    String key = property.getName();
                    //默认PropertyDescriptor会有一个class对象，剔除之
                    if("class".equals(property.getName())){
                        return;
                    }
                    Method getter = property.getReadMethod();
                    if(getter!=null) {
                        Object value = getter.invoke(obj);
                        map.put(key, value);
                    }
                }catch (Exception e){
                    Log.error("Object to Map 设置属性时, 发生异常!", e);
                }
            });
            return map;
        }catch (Exception e){
            Log.error("Object to Map 发生异常!", e);
            return new HashMap(0);
        }
    }


}
