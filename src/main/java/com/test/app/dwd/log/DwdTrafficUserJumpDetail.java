package com.test.app.dwd.log;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 流量域用户跳出事务事实表
 * 主要任务: 过滤用户跳出明细数据。
 * 思路分析:
 * 1）筛选策略
 * 2) Flink CEP
 * 3）实现步骤
 *    （1）按照 mid 分组
 *    （2）定义 CEP 匹配规则
 *    （3）把匹配规则（Pattern）应用到流上
 *    （4）提取超时流
 *    （5）合并主流和超时流，写入 Kafka 调出明细主题
 *    （6）结果分析
 *
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
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

        //TODO 3. 从kafka dwd_traffic_page_log 主题读取日志数据封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLog = env.addSource(kafkaConsumer);

        //测试数据
       /* DataStreamSource<Object> kafkaDS = env.fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                        "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                        "\"detail\"},\"ts\":30000} "
        );*/

        //TODO 4. 转换结构
        SingleOutputStreamOperator<JSONObject> mappedStream = pageLog.map(JSON::parseObject);

        //TODO 5. 设置水位线，用于统计用户跳出
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        ));

        //TODO 6. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //TODO 7. 定义CEP匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId ==null;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                }
        ).within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L));

        //TODO 8. 把pattern应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 9. 提取匹配上的事件以及超时事件
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeoutTag") {

        };
        SingleOutputStreamOperator<JSONObject> flatSelectStream = patternStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long timeoutTimestamp, Collector<JSONObject> collector) throws Exception {
                        JSONObject element = map.get("first").get(0);
                        collector.collect(element);
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<JSONObject> collector) throws Exception {
                        JSONObject element = map.get("first").get(0);
                        collector.collect(element);

                    }
                }
        );
        DataStream<JSONObject> timeoutStream = flatSelectStream.getSideOutput(timeoutTag);

        //TODO 11. 合并两条流并将数据写出到kafka
        String targetTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(targetTopic);

        flatSelectStream.union(timeoutStream)
                        .map(JSONAware::toJSONString)
                        .addSink(kafkaProducer);
        env.execute();
    }
}
