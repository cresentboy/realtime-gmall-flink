package com.test.app.dwd.db;


import com.alibaba.fastjson.JSON;
import com.test.bean.CouponUserOrderBean;
import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

/**
 * 工具域优惠券使用（下单）事务事实表
 * 9.12.1 主要任务
 * 	读取优惠券领用表数据，筛选优惠券下单数据，写入 Kafka 优惠券下单主题。
 * 9.12.2 思路分析
 * 1）知识储备
 * 用户使用优惠券下单时，优惠券领用表的 using_time 字段会更新为下单时间，因此优惠券下单数据应满足两个条件：① 操作类型为 update；② 修改了 using_time 字段。
 * （2）Flink SQL 的 Table 类型变量转化为 DataStream 有四类 API
 * ① toAppendStream
 * ② toDataStream
 * ③ toChangelogStream
 * ④ toRetractStream
 * （3）将流转化为动态表
 * ① 目前版本 Flink 只提供了两种 API
 * 	fromChangelogStream
 * 	fromDataStream
 * ② 应用场景
 * a）fromDataStream 不可用于包含删除和更新数据的流向 Table 的转化，否则报错
 * b）fromChangelogStream 可用于包含删除和更新数据流向 Table 的转化
 * （4）本项目 DWD 层涉及到的流和表转化
 * INSERT 操作的流和表没有回撤数据，不需要考虑去重问题。
 * ① 从 Kafka 读取的 ODS 层数据操作类型均为 INSERT；
 * ② 只含 INSERT 操作的数据和 Lookup 表关联后的数据同样只有 INSERT 操作
 * 所以，本项目中流和表的转化不用考虑去重，无须额外处理。
 * 2）执行步骤
 * （1）筛选优惠券领用数据封装为表。
 * 筛选操作类型为 update 的数据。
 * （2）在流中筛选优惠券领取数据。
 * 判断是否修改了 using_time 字段。
 * （3）封装为表，写入 Kafka 优惠券使用（下单）事实主题。
 */
public class DwdToolCouponOrder {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db` (\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`data` map<string, string>,\n" +
                "`type` string,\n" +
                "`old` string,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_order"));

        // TODO 4. 读取优惠券领用表数据，封装为流
        Table couponUseOrder = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['coupon_id'] coupon_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id,\n" +
                "data['using_time'] using_time,\n" +
                "`old`,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and `type` = 'update'\n");
        DataStream<CouponUserOrderBean> couponUseOrderDS = tableEnv.toAppendStream(couponUseOrder, CouponUserOrderBean.class);

        //TODO 5. 过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUserOrderBean> filteredDS = couponUseOrderDS.filter(
                couponUserOrderBean -> {
                    String old = couponUserOrderBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("using_time");
                    }
                    return true;
                }
        );
        Table resultTable = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 6. 建立upsert-kafka dwd_tool_coupon_order表
        tableEnv.executeSql(
                "create table dwd_tool_coupon_order(\n" +
                        "id string,\n" +
                        "coupon_id string,\n" +
                        "user_id string,\n" +
                        "order_id string,\n" +
                        "date_id string,\n" +
                        "order_time string,\n" +
                        "ts string,\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_order")
        );
        // TODO 7. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id,\n" +
                "coupon_id,\n" +
                "user_id,\n" +
                "order_id,\n" +
                "date_id,\n" +
                "using_time order_time,\n" +
                "ts from result_table");

        env.execute();
    }
}
