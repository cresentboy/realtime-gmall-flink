package com.test.app.dwd.db;

import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 工具域优惠券使用(支付)事务事实表
 *  主要任务
 * 	读取优惠券领用表数据，筛选优惠券支付数据，写入 Kafka 优惠券支付主题。
 *  思路分析
 * 	用户使用优惠券支付时，优惠券领用表的 used_time 字段会更新为支付时间，因此优惠券支付数据应满足两个条件：（1）操作类型为 update；（2）修改了 used_time 字段。使用优惠券支付后，优惠券领用表数据就不会再发生变化，所以在操作类型为 update 的前提下，只要 used_time 不为 null，就可以断定本次操作修改的是 used_time 字段。
 */
public class DwdToolCouponPay {
    public static void main(String[] args) {
        //TODO 1. 环境准备
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
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_pay"));

        // TODO 4. 读取优惠券领用表数据，筛选优惠券使用（支付）数据
        Table couponUsePay = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['coupon_id'] coupon_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "date_format(data['used_time'],'yyyy-MM-dd') date_id,\n" +
                "data['used_time'] used_time,\n" +
                "`old`,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and `type` = 'update'\n" +
                "and data['used_time'] is not null");

        tableEnv.createTemporaryView("coupon_use_pay", couponUsePay);

        // TODO 5. 建立 Upsert-Kafka dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_pay(\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "date_id string,\n" +
                "payment_time string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_pay"));

        // TODO 6. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_pay select " +
                "id,\n" +
                "coupon_id,\n" +
                "user_id,\n" +
                "order_id,\n" +
                "date_id,\n" +
                "used_time payment_time,\n" +
                "ts from coupon_use_pay");

    }
}
