package com.test.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.test.utils.DateFormatUtil;
import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * 流量域独立访客事务事实表
 * 主要任务: 过滤页面数据中的独立访客访问记录。
 *思路分析:
 * 1）过滤 last_page_id 不为 null 的数据
 * 2）筛选独立访客记录
 *3）状态存活时间设置
 *
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 4. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30*1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1),Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3.从kafka dwd_traffic_page_log主题读取日志数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLog = env.addSource(kafkaConsumer);

        //TODO 4.转换结构
        SingleOutputStreamOperator<JSONObject> mappedStream = pageLog.map(JSON::parseObject);

        //TODO 5.过滤last_page_id 不为null的数据
        /**
         * 独立访客数据对应的页面必然是会话起始页面，last_page_id 必为 null。
         * 过滤 last_page_id != null 的数据，减小数据量，提升计算效率。
         */
        SingleOutputStreamOperator<JSONObject> firstPageStream = mappedStream.filter(
                jsonObject -> jsonObject.getJSONObject("page")
                        .getString("last_page_id") == null
        );

        //TODO 6. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = firstPageStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //TODO 7. 通过flink状态编程过滤独立访客记录
        /**
         * 运用 Flink 状态编程，为每个 mid 维护一个键控状态，记录末次登录日期。
         * 如果末次登录日期为 null 或者不是今日，则本次访问是该 mid 当日首次访问，保留数据，将末次登录日期更新为当日。
         * 否则不是当日首次访问，丢弃数据。
         */
        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {
                    private ValueState<String> lastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last_visit_dt", String.class);
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1L))
                                        // 设置在创建和更新状态时更新存活时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        lastVisitDt = getRuntimeContext().getState(valueStateDescriptor);

                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String visitDt = DateFormatUtil.toDate(jsonObject.getLong("ts"));
                        String lastDt = lastVisitDt.value();

                        if (lastDt == null || !lastDt.equals(visitDt)) {
                            lastVisitDt.update(visitDt);
                            return true;
                        }
                        return false;
                    }
                }
        );

        //TODO 8. 将独立访客数据写入Kafka dwd_traffic_unique_visitor_detail 主题
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(targetTopic);
        filteredStream.map(JSONAware::toJSONString).addSink(kafkaProducer);

        //TODO 9.启动任务
        env.execute();


    }
}
