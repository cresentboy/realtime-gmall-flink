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
 * 工具域优惠券领取事务事实表
 *  主要任务
 * 读取优惠券领用数据，写入 Kafka 优惠券领用主题
 *  思路分析
 * 用户领取优惠券后，业务数据库的优惠券领用表会新增一条数据，因此操作类型为 insert 的数据即为优惠券领取数据。
 */
public class DwdToolCouponGet {
    public static void main(String[] args) {
        //TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1),Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3. 从Kafka读取业务数据，封装为flink SQL表
        tableEnv.executeSql(
                "create table `topic_db`(\n" +
                        "`database` string,\n" +
                        "`table` string,\n" +
                        "`data` map<string, string>,\n" +
                        "`type` string,\n" +
                        "`ts` string\n" +
                        ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_get")
        );

        //TODO 4. 读取优惠券领用数据，封装为表
        Table resultTable = tableEnv.sqlQuery(
                "select\n" +
                        "data['id'],\n" +
                        "data['coupon_id'],\n" +
                        "data['user_id'],\n" +
                        "date_format(data['get_time'],'yyyy-MM-dd') date_id,\n" +
                        "data['get_time'],\n" +
                        "ts\n" +
                        "from topic_db\n" +
                        "where `table` = 'coupon_use'\n" +
                        "and `type` = 'insert'\n"
        );

        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 5. 建立upsert-kafka dwd_tool_coupon_get表
        tableEnv.executeSql(
                "create table dwd_tool_coupon_get (\n" +
                        "id string,\n" +
                        "coupon_id string,\n" +
                        "user_id string,\n" +
                        "date_id string,\n" +
                        "get_time string,\n" +
                        "ts string,\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_get")
        );

        //TODO 6. 将数据写入upsert-kafka表
        tableEnv.executeSql(
                "" +
                        "insert into dwd_tool_coupon_get select * from result_table"
        );


    }
}
