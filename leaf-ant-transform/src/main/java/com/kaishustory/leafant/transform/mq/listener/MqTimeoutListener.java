package com.kaishustory.leafant.transform.mq.listener;

import com.kaishustory.leafant.common.utils.Log;
import com.kaishustory.leafant.transform.mq.model.MqEvent;
import com.kaishustory.leafant.transform.mq.service.MqTransformService;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.DelayQueue;

/**
 * MQ转发延迟处理
 *
 * @author liguoyang
 * @create 2019-09-02 11:34
 **/
@Component
public class MqTimeoutListener {

    /**
     * 延迟队列
     */
    private DelayQueue<MqEvent> delayQueue = new DelayQueue<>();
    /**
     * 应用
     */
    @Value("${spring.application.name}")
    private String app;
    /**
     * 环境
     */
    @Value("${spring.profiles.active}")
    private String env;
    /**
     * MQ GroupID
     */
    @Value("${mq.timer.groupId}")
    private String groupId;
    /**
     * MQ处理
     */
    @Autowired
    private MqTransformService mqTransformService;

    /**
     * 写入延迟队列
     *
     * @param event
     */
    public void addQueue(MqEvent event) {
        delayQueue.add(event);
    }

    /**
     * 读取延迟队列
     *
     * @return
     */
    @SneakyThrows
    public MqEvent takeQueue() {
        return delayQueue.take();
    }

    /**
     * 监听MQ延迟处理事件
     */
    @PostConstruct
    public void listener() {

        new Thread(() -> {
            while (true) {
                MqEvent event = takeQueue();
                Log.info("【MQ】收到延迟处理通知。event：{}", event);
                // 关闭延迟标志
                event.getConfig().setTimeout(0);
                // MQ转发处理
                mqTransformService.eventHandle(event.getConfig(), event.getEvents());
            }
        }).start();

    }
}
