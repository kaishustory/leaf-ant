
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


import java.text.DecimalFormat;

/**
 * 计时器
 *
 * @author liguoyang
 */
public class Time {


    /**
     * 任务名称
     */
    private String name;

    /**
     * 开始时间
     */
    private long beginTime;

    /**
     * 结束时间
     */
    private long endTime;

    /**
     * 开始记时
     *
     * @param name 名称
     * @param name 人物名称
     */
    public Time(String name) {
        this.name = name;
        this.beginTime = System.currentTimeMillis();
        Log.info(name + " 开始处理");
    }

    /**
     * 结束记时
     */
    public void end() {
        this.endTime = System.currentTimeMillis();
        float time = (endTime - beginTime);
        Log.info(name + " 处理完成");
        Log.info(name + " 耗时：" + new DecimalFormat("#.##").format(time) + "/ms");
    }

}
