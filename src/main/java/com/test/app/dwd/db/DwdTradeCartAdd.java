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

import java.time.ZoneId;

/**
 * 交易域加购事务事实表
 * 主要任务:提取加购操作生成加购表，并将字典表中的相关维度退化到加购表中，写出到 Kafka 对应主题。
 *思路分析:
 * 1）维度关联（维度退化）实现策略分析
 * 本章业务事实表的构建全部使用 FlinkSQL 实现，字典表数据存储在 MySQL 的业务数据库中，
 * 要做维度退化，就要将这些数据从 MySQL 中提取出来封装成 FlinkSQL 表，
 * Flink 的 JDBC SQL Connector 可以实现我们的需求。
 *
 *2）知识储备
 * JDBC SQL Connector:JDBC 连接器可以让 Flink 程序从拥有 JDBC 驱动的任意关系型数据库中读取数据或将数据写入数据库。
 *                    JDBC 连接器可以作为时态表关联中的查询数据源（又称维表）。目前，仅支持同步查询模式。
 * Lookup Cache: Lookup 缓存是用来提升有 JDBC 连接器参与的时态关联性能的
 * Lookup Join: Lookup Join 通常在 Flink SQL 表和外部系统查询结果关联时使用。这种关联要求一张表（主表）有处理时间字段，而另一张表（维表）由 Lookup 连接器生成。
 *                 	Lookup Join 做的是维度关联，而维度数据是有时效性的，那么我们就需要一个时间字段来对数据的版本进行标识。因此，Flink 要求我们提供处理时间用作版本字段。
 *
 *  3）执行步骤
 * 	（1）读取购物车表数据。
 *  （2）建立 Mysql-LookUp 字典表。
 * 	（3）关联购物车表和字典表，维度退化。
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {

        //TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设定table中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        //TODO 2.状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1),Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3. 从kafka读取业务数据， 封装为flinkSQL表
        tableEnv.executeSql("" +
                "create table topic_db(\n" +
                        "`database` string,\n" +
                        "`table` string,\n" +
                        "`type` string,\n" +
                        "`data` map<string, string>,\n" +
                        "`old` map<string, string>,\n" +
                        "`ts` string,\n" +
                        "`proc_time` as PROCTIME()\n" +
                        ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_cart_add")
        );

        //TOD 4.读取购物车表数据
        Table cartAdd = tableEnv.sqlQuery(
                "" +
                        "select\n" +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['source_id'] source_id,\n" +
                        "data['source_type'] source_type,\n" +
                        "if(`type` = 'insert',\n" +
                        "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                        "ts,\n" +
                        "proc_time\n" +
                        "from `topic_db` \n" +
                        "where `table` = 'cart_info'\n" +
                        "and (`type` = 'insert'\n" +
                        "or (`type` = 'update' \n" +
                        "and `old`['sku_num'] is not null \n" +
                        "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))"

        );
        tableEnv.createTemporaryView("cart_add",cartAdd);

        // TODO 5. 建立mysql——lookup 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDickUpDDL());

        //TODO 6. 关联两张表获得加购明细表
        Table resultTable = tableEnv.sqlQuery(
                "select\n" +
                        "cadd.id,\n" +
                        "user_id,\n" +
                        "sku_id,\n" +
                        "source_id,\n" +
                        "source_type,\n" +
                        "dic_name source_type_name,\n" +
                        "sku_num,\n" +
                        "ts\n" +
                        "from cart_add cadd\n" +
                        "left join base_dic for system_time as of cadd.proc_time as dic\n" +
                        "on cadd.source_type=dic.dic_code"

        );
        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 7. 建立Upsert-kafka dwd_trade_cart_add表
        tableEnv.executeSql(
                "" +
                        "create table dwd_trade_cart_add(\n" +
                        "id string,\n" +
                        "user_id string,\n" +
                        "sku_id string,\n" +
                        "source_id string,\n" +
                        "source_type_code string,\n" +
                        "source_type_name string,\n" +
                        "sku_num string,\n" +
                        "ts string,\n" +
                        "primary key(id) not enforced\n" +
                        ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add")

        );

        //TODO 8. 将关联结果写入upsert-kafka表
        tableEnv.executeSql(
                "" + "insert into dwd_trade_cart_add select * from result_table"
        );
    }
}
