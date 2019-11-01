package com.kaishustory.leafant.transform.mq.model;

import com.kaishustory.leafant.common.model.Event;
import com.kaishustory.leafant.common.model.MqSyncConfig;
import lombok.Data;

import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * MQ消息
 *
 * @author liguoyang
 * @create 2019-10-11 1:25 AM
 **/
@Data
public class MqEvent implements Delayed {

    /**
     * 执行时间
     */
    private long runTime;
    /**
     * 配置
     */
    private MqSyncConfig config;
    /**
     * 事件列表
     */
    private List<Event> events;

    public MqEvent() {
    }

    public MqEvent(long runTime, MqSyncConfig config, List<Event> events) {
        this.runTime = runTime;
        this.config = config;
        this.events = events;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return runTime - System.currentTimeMillis();
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.runTime, ((MqEvent) o).runTime);
    }

}
