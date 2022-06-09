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
 * 交易域支付成功事务事实表
 * 主要任务:
 * 从 Kafka topic_db主题筛选支付成功数据、从dwd_trade_order_detail主题中读取订单事实数据、MySQL-LookUp字典表，
 * 关联三张表形成支付成功宽表，写入 Kafka 支付成功主题。
 * 思路分析:
 * 1）获取订单明细数据
 * 用户必然要先下单才有可能支付成功，因此支付成功明细数据集必然是订单明细数据集的子集。
 * 2）筛选支付表数据
 * 获取支付类型、回调时间（支付成功时间）、支付成功时间戳。
 * 3）构建 MySQL-LookUp 字典表
 * 4）关联上述三张表形成支付成功宽表，写入 Kafka 支付成功主题
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3, Time.days(1),Time.minutes(1)
                )
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //DOTO 3. 读取kafka dwd_trade_order_detail主题数据，封装为flink sql表
        tableEnv.executeSql(
                "" +
                        "create table dwd_trade_order_detail(\n" +
                        "id string,\n" +
                        "order_id string,\n" +
                        "user_id string,\n" +
                        "sku_id string,\n" +
                        "sku_name string,\n" +
                        "province_id string,\n" +
                        "activity_id string,\n" +
                        "activity_rule_id string,\n" +
                        "coupon_id string,\n" +
                        "date_id string,\n" +
                        "create_time string,\n" +
                        "source_id string,\n" +
                        "source_type_code string,\n" +
                        "source_type_name string,\n" +
                        "sku_num string,\n" +
                        "split_original_amount string,\n" +
                        "split_activity_amount string,\n" +
                        "split_coupon_amount string,\n" +
                        "split_total_amount string,\n" +
                        "ts string,\n" +
                        "row_op_ts timestamp_ltz(3)\n" +
                        ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_pay_detail_suc")
        );

        //DOTO 4. 从kafka读取业务数据， 封装为flinkSQL表
        tableEnv.executeSql(
                "create table topic_db(" +
                        "`database` String,\n" +
                        "`table` String,\n" +
                        "`type` String,\n" +
                        "`data` map<String, String>,\n" +
                        "`old` map<String, String>,\n" +
                        "`proc_time` as PROCTIME(),\n" +
                        "`ts` string\n" +
                        ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_pay_detail_suc")
        );

        // TODO 5.  筛选支付成功数据
        tableEnv.sqlQuery(
                "select\n" +
                        "data['user_id'] user_id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "`proc_time`,\n" +
                        "ts\n" +
                        "from topic_db\n" +
                        "where `table` = 'payment_info'\n"
//                +
//                "and `type` = 'update'\n" +
//                "and data['payment_status']='1602'"
        );
        tableEnv.executeSql(MysqlUtil.getBaseDickUpDDL());

        //TODO 7. 关联3张表获得支付成功宽表
        Table resultTable = tableEnv.sqlQuery(
                "" +
                        "select\n" +
                        "od.id order_detail_id,\n" +
                        "od.order_id,\n" +
                        "od.user_id,\n" +
                        "od.sku_id,\n" +
                        "od.sku_name,\n" +
                        "od.province_id,\n" +
                        "od.activity_id,\n" +
                        "od.activity_rule_id,\n" +
                        "od.coupon_id,\n" +
                        "pi.payment_type payment_type_code,\n" +
                        "dic.dic_name payment_type_name,\n" +
                        "pi.callback_time,\n" +
                        "od.source_id,\n" +
                        "od.source_type_code,\n" +
                        "od.source_type_name,\n" +
                        "od.sku_num,\n" +
                        "od.split_original_amount,\n" +
                        "od.split_activity_amount,\n" +
                        "od.split_coupon_amount,\n" +
                        "od.split_total_amount split_payment_amount,\n" +
                        "pi.ts,\n" +
                        "od.row_op_ts row_op_ts\n" +
                        "from payment_info pi\n" +
                        "join dwd_trade_order_detail od\n" +
                        "on pi.order_id = od.order_id\n" +
                        "left join `base_dic` for system_time as of pi.proc_time as dic\n" +
                        "on pi.payment_type = dic.dic_code"
        );
        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 8. 创建kafka dwd_trade_pay_detail 表
        tableEnv.executeSql(
                "create table dwd_trade_pay_detail_suc(\n" +
                        "order_detail_id string,\n" +
                        "order_id string,\n" +
                        "user_id string,\n" +
                        "sku_id string,\n" +
                        "sku_name string,\n" +
                        "province_id string,\n" +
                        "activity_id string,\n" +
                        "activity_rule_id string,\n" +
                        "coupon_id string,\n" +
                        "payment_type_code string,\n" +
                        "payment_type_name string,\n" +
                        "callback_time string,\n" +
                        "source_id string,\n" +
                        "source_type_code string,\n" +
                        "source_type_name string,\n" +
                        "sku_num string,\n" +
                        "split_original_amount string,\n" +
                        "split_activity_amount string,\n" +
                        "split_coupon_amount string,\n" +
                        "split_payment_amount string,\n" +
                        "ts string,\n" +
                        "row_op_ts timestamp_ltz(3),\n" +
                        "primary key(order_detail_id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc")
        );

        //TODO 9. 将关联结果写入upsert-kafka 表
        tableEnv.executeSql(
                "" +
                        "insert into dwd_trade_pay_detail_suc select * from result_table"
        );


    }
}
