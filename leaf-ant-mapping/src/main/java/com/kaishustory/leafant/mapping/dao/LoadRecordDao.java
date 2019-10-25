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

package com.kaishustory.leafant.mapping.dao;

import com.kaishustory.leafant.mapping.model.LoadRecord;
import com.kaishustory.leafant.mapping.model.LoadStats;
import com.kaishustory.leafant.mapping.model.LoadStatsResult;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

import static com.kaishustory.leafant.mapping.model.LoadRecord.*;

/**
 * 初始化记录Dao
 *
 * @author liguoyang
 * @create 2019-09-11 10:50
 **/
@Component
public class LoadRecordDao {

    /**
     * Mongo
     */
    @Resource(name = "mappingMongoTemplate")
    private MongoTemplate mongoTemplate;

    private String collection = "load_record";

    /**
     * 创建初始化记录
     *
     * @param target    目标
     * @param mappingId 映射ID
     * @param mode      模式
     * @param rds       数据库实例
     * @param database  数据库
     * @param table     表
     * @param start     起始位置
     * @param end       截止位置
     * @param page      页号
     * @param pageSize  每页条数
     */
    public LoadRecord addRecord(String target, String mappingId, String mode, String rds, String database, String table, long start, long end, int page, int pageSize) {
        LoadRecord loadRecord = new LoadRecord(target, mappingId, mode, rds, database, table, start, end, page, pageSize);
        mongoTemplate.save(loadRecord, collection);
        return loadRecord;
    }

    /**
     * 按ID查询初始化记录
     *
     * @param id ID
     * @return 初始化记录
     */
    public LoadRecord findRecordById(String id) {
        return mongoTemplate.findById(id, LoadRecord.class, collection);
    }

    /**
     * 按MQID查询初始化记录
     *
     * @param mqid MQID
     * @return 初始化记录
     */
    public LoadRecord findRecordByMqid(String mqid) {
        return mongoTemplate.findOne(Query.query(Criteria.where("mqid").is(mqid)), LoadRecord.class, collection);
    }

    /**
     * 查询初始化记录
     *
     * @param target    目标数据源
     * @param mappingId 映射ID
     * @param table     表
     * @param page      页号
     * @return 初始化记录
     */
    public LoadRecord findRecordByPage(String mode, String target, String mappingId, String table, int page, int pageSize) {
        return mongoTemplate.findOne(Query.query(Criteria
                .where("mappingId").is(mappingId)
                .and("target").is(target)
                .and("table").is(table)
                .and("mode").is(mode)
                .and("page").is(page)
                .and("pageSize").is(pageSize)
        ), LoadRecord.class, collection);
    }

    /**
     * 统计初始化结果
     *
     * @param target    目标数据源
     * @param mappingId 映射ID
     * @param table     表
     * @return 初始化统计结果
     */
    public LoadStats findLoadStats(String target, String mappingId, String table) {
        LoadStats stats = new LoadStats();
        AggregationResults<LoadStatsResult> rs = mongoTemplate.aggregate(
                Aggregation.newAggregation(
                        Aggregation.match(Criteria.where("mappingId").is(mappingId).and("target").is(target).and("table").is(table)),
                        Aggregation.group("status").count().as("statusCount")),
                collection,
                LoadStatsResult.class
        );

        rs.getMappedResults().forEach(r -> {
            if (STATUS_WAIT.equals(r.get_id())) {
                stats.setWait(r.getStatusCount());
            } else if (STATUS_SEND.equals(r.get_id())) {
                stats.setSend(r.getStatusCount());
            } else if (STATUS_SUCCESS.equals(r.get_id())) {
                stats.setSuceess(r.getStatusCount());
            } else if (STATUS_FAIL.equals(r.get_id())) {
                stats.setFail(r.getStatusCount());
            }
        });
        return stats;
    }

    /**
     * 更新MQID和已发送状态
     *
     * @param id   ID
     * @param mqid MQID
     */
    public void updateRecordSendMqidById(String id, List<String> mqid, int size) {
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(id)),
                Update.update("status", STATUS_SEND)
                        .set("mqid", mqid)
                        .set("size", size)
                        .currentDate("updateTime"),
                collection);
    }

    /**
     * 更新成功状态
     *
     * @param mqid MQID
     */
    public void updateRecordSuccessByMqid(String mqid) {
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("mqid").is(mqid)),
                Update
                        .update("status", STATUS_SUCCESS)
                        .currentDate("updateTime")
                        .unset("errmsg")
                ,
                collection);
    }

    /**
     * 更新成功状态
     *
     * @param id ID
     */
    public void updateRecordSuccessById(String id) {
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(id)),
                Update
                        .update("status", STATUS_SUCCESS)
                        .currentDate("updateTime")
                        .unset("errmsg")
                ,
                collection);
    }

    /**
     * 更新失败状态
     *
     * @param mqid MQID
     */
    public void updateRecordFailByMqid(String mqid, String errmsg) {
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("mqid").is(mqid)),
                Update
                        .update("status", STATUS_FAIL)
                        .set("errmsg", errmsg)
                        .currentDate("updateTime"),
                collection);
    }

    /**
     * 更新失败状态
     *
     * @param id ID
     */
    public void updateRecordFailById(String id, String errmsg) {
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(id)),
                Update
                        .update("status", STATUS_FAIL)
                        .set("errmsg", errmsg)
                        .currentDate("updateTime"),
                collection);
    }

}
