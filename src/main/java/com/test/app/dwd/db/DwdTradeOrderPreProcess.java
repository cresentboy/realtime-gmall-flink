package com.test.app.dwd.db;

import com.test.utils.KafkaUtil;
import com.test.utils.MysqlUtil;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 *交易域订单预处理表
 *主要任务: 关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表和字典表（维度业务表）形成订单预处理表，写入 Kafka 对应主题。
 *思路分析:
 *1）知识储备:
 * （1）left join 实现过程:
 * 假设 A 表作为主表与 B 表做等值左外联。当 A 表数据进入算子，而 B 表数据未至时会先生成一条 B 表字段均为 null 的关联数据ab1，其标记为 +I。
 * 其后，B 表数据到来，会先将之前的数据撤回，即生成一条与 ab1 内容相同，但标记为 -D 的数据，再生成一条关联后的数据，标记为 +I。这样生成的动态表对应的流称之为回撤流。
 * （2）Kafka SQL Connector:
 * Kafka SQL Connector 分为 Kafka SQL Connector 和 Upsert Kafka SQL Connector
 * Upsert Kafka Connector支持以 upsert 方式从 Kafka topic 中读写数据,要求表必须有主键；将 INSERT/UPDATE_AFTER 数据作为正常的 Kafka 消息写入，并将 DELETE 数据以 value 为空的 Kafka 消息写入（表示对应 key 的消息被删除）。
 * Kafka Connector支持从 Kafka topic 中读写数据,要求表不能有主键，不能消费带有 Upsert/Delete 操作类型数据的表
 *
 * 2）执行步骤
 * （1）从 Kafka topic_db 主题读取业务数据；
 * （2）筛选订单明细表数据；
 * （3）筛选订单表数据；
 * （4）筛选订单明细活动关联表数据；
 * （5）筛选订单明细优惠券关联表数据；
 * （6）建立 MySQL-Lookup 字典表；
 * （7）关联上述五张表获得订单宽表，写入 Kafka 主题
 *
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        //TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2. 启用状态后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atuguigu");
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        //TODO 3. 从kafka读取业务数据，封装为flink SQL表
        tableEnv.executeSql("create table topic_db(" +
                "`database` String,\n" +
                "`table` String,\n" +
                "`type` String,\n" +
                "`data` map<String, String>,\n" +
                "`old` map<String, String>,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_pre_process"));

        //TODO 4. 读取订单明细表数据
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "ts od_ts,\n" +
                "proc_time\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail",orderDetail);

        //DOTDO 5. 读取订单表数据
        Table orderInfo = tableEnv.sqlQuery(
                "select \n" +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['province_id'] province_id,\n" +
                        "data['operate_time'] operate_time,\n" +
                        "data['order_status'] order_status,\n" +
                        "`type`,\n" +
                        "`old`,\n" +
                        "ts oi_ts\n" +
                        "from `topic_db`\n" +
                        "where `table` = 'order_info'\n" +
                        "and (`type` = 'insert' or `type` = 'update')"

        );
        tableEnv.createTemporaryView("order_info",orderInfo);

        //TODO 6. 读取订单明细活动关联表数据
        Table orderDetailActivtiy = tableEnv.sqlQuery(
                "select \n" +
                        "data['order_detail_id'] order_detail_id,\n" +
                        "data['activity_id'] activity_id,\n" +
                        "data['activity_rule_id'] activity_rule_id\n" +
                        "from `topic_db`\n" +
                        "where `table` = 'order_detail_activity'\n" +
                        "and `type` = 'insert'\n"
        );
        tableEnv.createTemporaryView("order_detail_activity",orderDetailActivtiy);

        //TODO 7. 读取订单明细优惠券关联表数据
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select\n" +
                        "data['order_detail_id'] order_detail_id,\n" +
                        "data['coupon_id'] coupon_id\n" +
                        "from `topic_db`\n" +
                        "where `table` = 'order_detail_coupon'\n" +
                        "and `type` = 'insert'\n"
        );
        tableEnv.createTemporaryView("order_detail_coupon",orderDetailCoupon);

        //TODO 8. 建立mysql-lookup字典表
        tableEnv.executeSql(MysqlUtil.getBaseDickUpDDL());

        //TODO 9.关联五张表获得订单明细表
        Table resultTable = tableEnv.sqlQuery(
                "select \n" +
                        "od.id,\n" +
                        "od.order_id,\n" +
                        "oi.user_id,\n" +
                        "oi.order_status,\n" +
                        "od.sku_id,\n" +
                        "od.sku_name,\n" +
                        "oi.province_id,\n" +
                        "act.activity_id,\n" +
                        "act.activity_rule_id,\n" +
                        "cou.coupon_id,\n" +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                        "od.create_time,\n" +
                        "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                        "oi.operate_time,\n" +
                        "od.source_id,\n" +
                        "od.source_type,\n" +
                        "dic.dic_name source_type_name,\n" +
                        "od.sku_num,\n" +
                        "od.split_original_amount,\n" +
                        "od.split_activity_amount,\n" +
                        "od.split_coupon_amount,\n" +
                        "od.split_total_amount,\n" +
                        "oi.`type`,\n" +
                        "oi.`old`,\n" +
                        "od.od_ts,\n" +
                        "oi.oi_ts,\n" +
                        "current_row_timestamp() row_op_ts\n" +
                        "from order_detail od \n" +
                        "join order_info oi\n" +
                        "on od.order_id = oi.id\n" +
                        "left join order_detail_activity act\n" +
                        "on od.id = act.order_detail_id\n" +
                        "left join order_detail_coupon cou\n" +
                        "on od.id = cou.order_detail_id\n" +
                        "left join `base_dic` for system_time as of od.proc_time as dic\n" +
                        "on od.source_type = dic.dic_code"
        );
        tableEnv.createTemporaryView("reslult_table",resultTable);

        //TODO 10. 建立upsert-kafka dwd_trade_order_pre_process表
        tableEnv.executeSql(
                "" +
                        "create table dwd_trade_order_pre_process(\n" +
                        "id string,\n" +
                        "order_id string,\n" +
                        "user_id string,\n" +
                        "order_status string,\n" +
                        "sku_id string,\n" +
                        "sku_name string,\n" +
                        "province_id string,\n" +
                        "activity_id string,\n" +
                        "activity_rule_id string,\n" +
                        "coupon_id string,\n" +
                        "date_id string,\n" +
                        "create_time string,\n" +
                        "operate_date_id string,\n" +
                        "operate_time string,\n" +
                        "source_id string,\n" +
                        "source_type string,\n" +
                        "source_type_name string,\n" +
                        "sku_num string,\n" +
                        "split_original_amount string,\n" +
                        "split_activity_amount string,\n" +
                        "split_coupon_amount string,\n" +
                        "split_total_amount string,\n" +
                        "`type` string,\n" +
                        "`old` map<string,string>,\n" +
                        "od_ts string,\n" +
                        "oi_ts string,\n" +
                        "row_op_ts timestamp_ltz(3),\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process")
        );

        //TODO 11.将关联结果写入upsert-kafka表
        tableEnv.executeSql(
                "" +
                        "insert into dwd_trade_order_pre_process \n" +
                        "select * from result_table"
        ).print();

        env.execute();

    }
}
