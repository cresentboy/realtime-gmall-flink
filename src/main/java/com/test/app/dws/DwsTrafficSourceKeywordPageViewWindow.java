package com.test.app.dws;

import com.test.app.func.KeywordUDTF;
import com.test.bean.KeywordBean;
import com.test.common.GmallConstant;
import com.test.utils.ClickHouseUtil;
import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域来源关键词粒度页面浏览各窗口汇总表
 *  主要任务:
 * 	从 Kafka 页面浏览明细主题读取数据，过滤搜索行为，使用自定义 UDTF（一进多出）函数对搜索内容分词。统计各窗口各关键词出现频次，写入 ClickHouse。
 *  思路分析:
 *  本程序将使用 FlinkSQL 实现。分词是个一进多出的过程，需要一个 UDTF 函数来实现，FlinkSQL 没有提供相关的内置函数，所以要自定义 UDTF 函数。
 * 	自定义函数的逻辑在代码中实现，要完成分词功能，需要导入相关依赖，此处将借助 IK 分词器完成分词。
 * 	最终要将数据写入 ClickHouse，需要补充相关依赖，封装 ClickHouse 工具类和方法
 *  1）分词处理
 * 分词处理分为八个步骤，如下。
 * （1）创建分词工具类
 * 	定义分词方法，借助 IK 分词器提供的工具将输入的关键词拆分成多个词，返回一个 List 集合。
 * （2）创建自定义函数类
 * 	继承 Flink 的 TableFunction 类，调用分词工具类的分词方法，实现分词逻辑。
 * （3）注册函数
 * （4）从 Kafka 页面浏览明细主题读取数据并设置水位线
 * （5）过滤搜索行为
 * 	满足以下三个条件的即为搜索行为数据：
 * ① page 字段下 item 字段不为 null；
 * ② page 字段下 last_page_id 为 search；
 * ③ page 字段下 item_type 为 keyword。
 * （6）分词
 * （7）分组、开窗、聚合计算
 * 按照拆分后的关键词分组。统计每个词的出现频次，补充窗口起始时间、结束时间和关键词来源（source）字段。调用 unix_timestamp() 函数获取以秒为单位的当前系统时间戳，转为毫秒（*1000），作为 ClickHouse 表的版本字段，用于数据去重。
 * （8）将动态表转换为流
 *
 * 2）将数据写入 ClickHouse
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 2.检查点设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3, Time.days(1L), Time.minutes(1L)
                )
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_traffic_page_log 主题中读取页面浏览日志数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_log(\n" +
                "`common` map<string, string>,\n" +
                "`page` map<string, string>,\n" +
                "`ts` bigint,\n" +
                "row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '3' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL(topic, groupId));

        // TODO 4. 从表中过滤搜索行为
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "page['item'] full_word,\n" +
                "row_time\n" +
                "from page_log\n" +
                "where page['item'] is not null\n" +
                "and page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("search_table", searchTable);

        // TODO 5. 使用自定义的UDTF函数对搜索的内容进行分词
        Table splitTable = tableEnv.sqlQuery("select\n" +
                "keyword,\n" +
                "row_time \n" +
                "from search_table,\n" +
                "lateral table(ik_analyze(full_word))\n" +
                "as t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);

        // TODO 6. 分组、开窗、聚合计算
        Table KeywordBeanSearch = tableEnv.sqlQuery("select\n" +
                "DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                GmallConstant.KEYWORD_SEARCH + "' source,\n" +
                "keyword,\n" +
                "count(*) keyword_count,\n" +
                "UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),keyword");

        //TODO 7.将动态表装换为流
        DataStream<KeywordBean> keywordBeanDS = tableEnv.toAppendStream(KeywordBeanSearch, KeywordBean.class);

        //TODO 8. 将流中的数据写到clickhouse中
        SinkFunction<KeywordBean> jdbcSink = ClickHouseUtil.<KeywordBean>getJdbcSink(
                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?<?)"
        );
        keywordBeanDS.addSink(jdbcSink);

        env.execute();


    }
}
