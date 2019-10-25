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

package com.kaishustory.message.consumer;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

/**
 * 自动寻找端口
 **/
public class AutoPort {

    private static Set<Integer> bindPort = new HashSet<>();

    /**
     * 自动寻找可用端口
     *
     * @return 端口
     */
    public static int get() {
        synchronized (AutoPort.class) {
            for (int port = 2000; port < 26000; port++) {
                if (!bindPort.contains(port)) {
                    try {
                        Socket socket = new Socket("127.0.0.1", port);
                        socket.close();
                    } catch (IOException e) {
                        // 可用端口
                        bindPort.add(port);
                        return port;
                    }
                }
            }
            throw new RuntimeException("无法找到可用端口！");
        }
    }
}
