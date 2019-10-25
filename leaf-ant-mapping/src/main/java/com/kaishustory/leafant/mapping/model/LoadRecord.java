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

package com.kaishustory.leafant.mapping.model;

import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * 初始化记录
 *
 * @author liguoyang
 * @create 2019-09-10 19:18
 **/
@Data
public class LoadRecord {

    /**
     * 主键模式
     */
    public static final String MODE_ID = "id";
    /**
     * 下标模式
     */
    public static final String MODE_LIMIT = "limit";
    /**
     * 待处理
     */
    public static final String STATUS_WAIT = "wait";
    /**
     * 已发送MQ
     */
    public static final String STATUS_SEND = "send";
    /**
     * 成功
     */
    public static final String STATUS_SUCCESS = "success";
    /**
     * 失败
     */
    public static final String STATUS_FAIL = "fail";
    /**
     * 主键
     */
    private String id;
    /**
     * MQID
     */
    private List<String> mqid;
    /**
     * 同步目标（ElasticSearch：es，Redis：redis，MQ：mq）
     */
    private String target;
    /**
     * 映射ID
     */
    private String mappingId;
    /**
     * 模式：
     * id：主键查询
     * limit：下标查询
     */
    private String mode;
    /**
     * 数据库实例
     */
    private String rds;
    /**
     * 数据库
     */
    private String database;
    /**
     * 表
     */
    private String table;
    /**
     * 开始位置
     */
    private long start;
    /**
     * 截止位置
     */
    private long end;
    /**
     * 页号
     */
    private int page;
    /**
     * 每页条数
     */
    private int pageSize;
    /**
     * 记录条数
     */
    private int size;
    /**
     * 加载状态
     * wait：待处理
     * send：已发送MQ
     * suceess：成功
     * fail：失败
     */
    private String status = STATUS_WAIT;
    /**
     * 异常信息
     */
    private String errmsg;
    /**
     * 更新时间
     */
    private Date updateTime = new Date();

    public LoadRecord() {
    }

    public LoadRecord(String target, String mappingId, String mode, String rds, String database, String table, long start, long end, int page, int pageSize) {
        this.target = target;
        this.mappingId = mappingId;
        this.mode = mode;
        this.rds = rds;
        this.database = database;
        this.table = table;
        this.start = start;
        this.end = end;
        this.page = page;
        this.pageSize = pageSize;
    }
}
