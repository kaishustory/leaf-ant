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

package com.kaishustory.leafant.subscribe.common.canal;

import com.kaishustory.leafant.subscribe.service.CanalMessageHandle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Canal MySQL事件监听注册
 *
 * @author liguoyang
 * @create 2019-07-09 15:02
 **/
@Service
public class CanalListenRegisterService {

    /**
     * canal 服务名称
     */
    @Value("${canal.destination}")
    private String destination;

    /**
     * Canal监听线程列表
     */
    private List<CanalListener> listenerList = new ArrayList<>();

    /**
     * Canal事件监听注册
     */
    public void canalListenRegister() {

        // 监听数据库
        Arrays.stream(destination.split(",")).forEach(server -> {

            // 监听处理
            CanalListener listener = new CanalListener(server, new CanalMessageHandle(server));

            // 监听线程
            Thread canalThread = new Thread(listener);
            // 线程名称
            canalThread.setName("canal-listen-" + server);
            // 记录监听
            listenerList.add(listener);
            // 线程启动
            canalThread.start();
        });

        // 服务停机前处理，等待已订阅数据处理完成
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                listenerList.forEach(CanalListener::stop))
        );
    }

}
