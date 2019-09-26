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

package com.kaishustory.message.common.zookeeper;

import com.google.common.collect.Sets;
import com.kaishustory.message.common.model.ZooRegisterMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

import static com.kaishustory.message.common.constants.MessageConstants.METHOD_ADD_COUSUMER;
import static com.kaishustory.message.common.constants.MessageConstants.METHOD_ADD_PRODUCER;

/**
 * Zookeeper 重连处理
 * @author liguoyang
 * @create 2019-08-08 12:03
 **/
@Slf4j
public class ZooReconn {

    /**
     * 注册方法列表
     */
    private static Set<ZooRegisterMethod> methods = Sets.newConcurrentHashSet();;

    /**
     * 重连处理
     */
    public static void reconn(){
        methods.forEach(method -> {
            ZooOpera zooOpera = new ZooOpera(method.getZooAddr());
            if(METHOD_ADD_COUSUMER.equals(method.getMethod())){
                zooOpera.addCousumer(method.getGroup(), method.getTopic(), method.getHost());
                log.info("重新注册消费者：Group：{}，Topic：{}，Host：{}", method.getGroup(), method.getTopic(), method.getHost());
            }else if(METHOD_ADD_PRODUCER.equals(method.getMethod())){
                zooOpera.addProducer(method.getGroup(), method.getTopic(), method.getHost());
                log.info("重新注册生产者：Group：{}，Topic：{}，Host：{}", method.getGroup(), method.getTopic(), method.getHost());
            }else {
                log.error("不支持的方法：{}", method.getMethod());
            }
        });
    }

    /**
     * 记录注册方法
     * @param method 方法
     */
    public static void add(ZooRegisterMethod method){
        if(!methods.contains(method)) {
            methods.add(method);
            if (methods.contains(method)) {
                log.info("记录注册节点：Method：{}，Group：{}，Topic：{}，Host：{}", method.getMethod(), method.getGroup(), method.getTopic(), method.getHost());
            } else {
                log.error("记录注册节点失败！：Method：{}，Group：{}，Topic：{}，Host：{}", method.getMethod(), method.getGroup(), method.getTopic(), method.getHost());
            }
        }
    }
}
