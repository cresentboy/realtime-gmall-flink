package com.test.app.dwd.db;

import com.sun.xml.internal.bind.v2.TODO;
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
 * 交易域退款成功事务事实表
 * 主要任务:
 * 1）从退款表中提取退款成功数据，并将字典表的 dic_name 维度退化到表中
 * 2）从订单表中提取退款成功订单数据
 * 3）从退单表中提取退款成功的明细数据
 *思路分析:
 * 1）建立 MySQL-Lookup 字典表
 * 	获取支付类型名称。
 * 2）读取退款表数据，筛选退款成功数据
 * 3）读取订单表数据，过滤退款成功订单数据
 * 4）筛选退款成功的退单明细数据
 * 5）关联四张表并写出到 Kafka 退款成功主题
 */
public class DwdTradeRefundPaySun {
    public static void main(String[] args) {
        //TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1),Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3. 从kafka读取topic_db数据，封装为flinkSQL表
        tableEnv.executeSql(
                "create table topic_db(" +
                        "`database` string,\n" +
                        "`table` string,\n" +
                        "`type` string,\n" +
                        "`data` map<string, string>,\n" +
                        "`old` map<string, string>,\n" +
                        "`proc_time` as PROCTIME(),\n" +
                        "`ts` string\n" +
                        ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_refund_pay_suc")
        );

        //TODO 4. 简历mysql-lookup字典表
        tableEnv.executeSql(MysqlUtil.getBaseDickUpDDL());

        //TODO 5. 读取退款表数据，并筛选退款成功数据
        Table refundPayment = tableEnv.sqlQuery(
                "select\n" +
                        "data['id'] id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "data['total_amount'] total_amount,\n" +
                        "proc_time,\n" +
                        "ts\n" +
                        "from topic_db\n" +
                        "where `table` = 'refund_payment'\n" +
//                "and `type` = 'update'\n" +
                        "and data['refund_status'] = '0702'\n"
//                        +
//                "and `old`['refund_status'] is not null"
        );
        tableEnv.createTemporaryView("refund_payment",refundPayment);

        //TODO 6. 读取订单表数据并过滤退款成功订单数据
        Table orderInfo = tableEnv.sqlQuery(
                "select\n" +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['province_id'] province_id,\n" +
                        "`old`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_info'\n" +
                        "and `type` = 'update'\n"
//                +
//                "and data['order_status']='1006'\n" +
//                "and `old`['order_status'] is not null"
        );
        tableEnv.createTemporaryView("order_info",orderInfo);

        //TODO 7. 读取退单表数据并过滤退款成功数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['refund_num'] refund_num,\n" +
                        "`old`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_refund_info'\n"
//                        +
//                        "and `type` = 'update'\n" +
//                        "and data['refund_status']='0705'\n" +
//                        "and `old`['refund_status'] is not null"
                // order_refund_info 表的 refund_status 字段值均为 null
        );
        tableEnv.createTemporaryView("order_refund_info",orderRefundInfo);

        //TODO 8. 关联四张表获得退款成功表
        Table resultTable = tableEnv.sqlQuery(
                "select\n" +
                        "rp.id,\n" +
                        "oi.user_id,\n" +
                        "rp.order_id,\n" +
                        "rp.sku_id,\n" +
                        "oi.province_id,\n" +
                        "rp.payment_type,\n" +
                        "dic.dic_name payment_type_name,\n" +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id,\n" +
                        "rp.callback_time,\n" +
                        "ri.refund_num,\n" +
                        "rp.total_amount,\n" +
                        "rp.ts,\n" +
                        "current_row_timestamp() row_op_ts\n" +
                        "from refund_payment rp \n" +
                        "left join \n" +
                        "order_info oi\n" +
                        "on rp.order_id = oi.id\n" +
                        "left join\n" +
                        "order_refund_info ri\n" +
                        "on rp.order_id = ri.order_id\n" +
                        "and rp.sku_id = ri.sku_id\n" +
                        "left join \n" +
                        "base_dic for system_time as of rp.proc_time as dic\n" +
                        "on rp.payment_type = dic.dic_code\n"
        );
        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 9. 创建upsert-kafka dwd_trade_refund_pay_suc表
        tableEnv.executeSql(
                "create table dwd_trade_refund_pay_suc(\n" +
                        "id string,\n" +
                        "user_id string,\n" +
                        "order_id string,\n" +
                        "sku_id string,\n" +
                        "province_id string,\n" +
                        "payment_type_code string,\n" +
                        "payment_type_name string,\n" +
                        "date_id string,\n" +
                        "callback_time string,\n" +
                        "refund_num string,\n" +
                        "refund_amount string,\n" +
                        "ts string,\n" +
                        "row_op_ts timestamp_ltz(3),\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_refund_pay_suc")
        );

        //TODO 10. 将关联结果写入upsert-kafka表
        tableEnv.executeSql("" + "insert into dwd_trade_refund_pay_suc select * from result_table" );

    }
}
