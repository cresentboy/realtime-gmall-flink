package com.test.app.dwd.db;

import com.test.utils.KafkaUtil;
import com.test.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域退单事务事实表
 * 主要任务:
 * 从 Kafka 读取业务数据，筛选退单表数据，筛选满足条件的订单表数据，建立 MySQL-Lookup 字典表，关联三张表获得退单明细宽表。
 *思路分析:
 * 1）筛选退单表数据
 * 	退单业务过程最细粒度的操作为一个订单中一个 SKU 的退单操作，退单表粒度与最细粒度相同，将其作为主表。
 * 2）筛选订单表数据并转化为流
 * 	获取 province_id。退单操作发生时，订单表的 order_status 字段值会由1002（已支付）更新为 1005（退款中）。订单表中的数据要满足三个条件：
 * （1）order_status 为 1005（退款中）；
 * （2）操作类型为 update；
 * （3）更新的字段为 order_status。
 * 该字段发生变化时，变更数据中 old 字段下 order_status 的值不为 null（为 1002）。
 * 3）建立 MySQL-Lookup 字典表
 * 	获取退款类型名称和退款原因类型名称。
 * 4）关联这几张表获得退单明细宽表，写入 Kafka 退单明细主题
 * 	主表中的数据都与退单业务相关，因此所有关联均用左外联即可。第二步是否对订单表数据筛选并不影响查询结果，提前对数据进行过滤是为了减少数据量，减少性能消耗。
 *
 */
public class DwdTradeOrderRefund {
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

        //TODO 3. 从kafka读取topic_db数据，封装flink SQL表
        tableEnv.executeSql(
                "create table topic_db(" +
                        "`database` string,\n" +
                        "`table` string,\n" +
                        "`type` string,\n" +
                        "`data` map<string, string>,\n" +
                        "`old` map<string, string>,\n" +
                        "`proc_time` as PROCTIME(),\n" +
                        "`ts` string\n" +
                        ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_refund")
        );

        //TODO 4. 读取退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select\n" +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['refund_type'] refund_type,\n" +
                        "data['refund_num'] refund_num,\n" +
                        "data['refund_amount'] refund_amount,\n" +
                        "data['refund_reason_type'] refund_reason_type,\n" +
                        "data['refund_reason_txt'] refund_reason_txt,\n" +
                        "data['create_time'] create_time,\n" +
                        "proc_time,\n" +
                        "ts\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_refund_info'\n" +
                        "and `type` = 'insert'\n"
        );
        tableEnv.createTemporaryView("order_refund_info",orderRefundInfo);

        //TODO 5. 读取订单表数据，筛选退单数据
        Table orderInforRefund = tableEnv.sqlQuery(
                "select\n" +
                        "data['id'] id,\n" +
                        "data['province_id'] province_id,\n" +
                        "`old`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_info'\n" +
                        "and `type` = 'update'\n" +
                        "and data['order_status']='1005'\n" +
                        "and `old`['order_status'] is not null"
        );
        tableEnv.createTemporaryView("order_info_refund",orderInforRefund);

        //TODO 6. 简历mysql-lookup字典表
        tableEnv.executeSql(MysqlUtil.getBaseDickUpDDL());

        //TODO 7.关联三张表获得退单宽表
        Table resultTable = tableEnv.sqlQuery(
                "select \n" +
                        "ri.id,\n" +
                        "ri.user_id,\n" +
                        "ri.order_id,\n" +
                        "ri.sku_id,\n" +
                        "oi.province_id,\n" +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id,\n" +
                        "ri.create_time,\n" +
                        "ri.refund_type,\n" +
                        "type_dic.dic_name,\n" +
                        "ri.refund_reason_type,\n" +
                        "reason_dic.dic_name,\n" +
                        "ri.refund_reason_txt,\n" +
                        "ri.refund_num,\n" +
                        "ri.refund_amount,\n" +
                        "ri.ts,\n" +
                        "current_row_timestamp() row_op_ts\n" +
                        "from order_refund_info ri\n" +
                        "left join \n" +
                        "order_info_refund oi\n" +
                        "on ri.order_id = oi.id\n" +
                        "left join \n" +
                        "base_dic for system_time as of ri.proc_time as type_dic\n" +
                        "on ri.refund_type = type_dic.dic_code\n" +
                        "left join\n" +
                        "base_dic for system_time as of ri.proc_time as reason_dic\n" +
                        "on ri.refund_reason_type=reason_dic.dic_code"
        );
        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 8.简历upsert-kafka dwd_trade_order_refund表
        tableEnv.executeSql(
                "create table dwd_trade_order_refund(\n" +
                        "id string,\n" +
                        "user_id string,\n" +
                        "order_id string,\n" +
                        "sku_id string,\n" +
                        "province_id string,\n" +
                        "date_id string,\n" +
                        "create_time string,\n" +
                        "refund_type_code string,\n" +
                        "refund_type_name string,\n" +
                        "refund_reason_type_code string,\n" +
                        "refund_reason_type_name string,\n" +
                        "refund_reason_txt string,\n" +
                        "refund_num string,\n" +
                        "refund_amount string,\n" +
                        "ts string,\n" +
                        "row_op_ts timestamp_ltz(3),\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_order_refund")
        );

        //TODO 9.将关联结果写入upsert-kafka 表
        tableEnv.executeSql(""+ "insert into dwd_trade_order_refund select * from result_table");
    }
}
