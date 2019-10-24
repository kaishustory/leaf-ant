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

package com.kaishustory.leafant.subscribe.common.utils;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池
 *
 * @author liguoyang
 * @create 2019-07-11 14:26
 **/
public class ThreadPoolTool {

    private final static ThreadPoolExecutor threadPool = new ThreadPoolExecutor(5, 500, 5, TimeUnit.MINUTES, new SynchronousQueue<>());

    public static void exec(Runnable runnable) {
        threadPool.execute(runnable);
    }
}
