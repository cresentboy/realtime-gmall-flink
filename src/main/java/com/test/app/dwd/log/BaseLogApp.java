package com.test.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.test.utils.DateFormatUtil;
import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;


import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.util.concurrent.TimeUnit;

/**
 * 流量域未经加工的事务事实表
 * 主要任务
 * 数据清洗（ETL）
 * 新老访客状态标记修复
 * 分流
 *
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2. 启用状态后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60* 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10,
                Time.of(3L, TimeUnit.DAYS),Time.of(1L,TimeUnit.MINUTES) ));
        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3. 从kafka读取主流数据
        String topic = "topic_log";
        String groupId = "base_log_consumer";
        DataStreamSource<String> source = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 4.数据清洗，转换结构
        //4.1 定义错误输出流
        OutputTag<String> dirtyStreamTag = new OutputTag<String>("dirtyStream") {

        };
        SingleOutputStreamOperator<String> cleanedStream = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(jsonStr);
                    collector.collect(jsonStr);
                } catch (Exception e) {
                    context.output(dirtyStreamTag, jsonStr);
                }
            }
        });

        // 4.2 将脏数据写出到kafka指定主题
        DataStream<String> dirtyStream = cleanedStream.getSideOutput(dirtyStreamTag);
        String dirtyTopic = "dirty_data";
        dirtyStream.addSink(KafkaUtil.getKafkaProducer(dirtyTopic));

        // 4.3 转换主流数据结构 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> mappedStream = cleanedStream.map(JSON::parseObject);

        // TODO 5. 新老访客状态标记修复
        //5.1 按照mid对数据进行分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getJSONObject("common").getString("mid"));

        //5.2 新老访客状态标记修复
        SingleOutputStreamOperator<JSONObject> fixedStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<String> firstViewDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                firstViewDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                        "lastLoginDt",
                        String.class
                ));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                String firstViewDt = firstViewDtState.value();
                Long ts = jsonObject.getLong("ts");
                String dt = DateFormatUtil.toDate(ts);
                if ("1".equals(isNew)) {
                    if (firstViewDt == null) {
                        firstViewDtState.update(dt);
                    } else {
                        if (!firstViewDt.equals(dt)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    if (firstViewDt == null) {
                        //将首次访问日期置为昨日
                        String yesterday = DateFormatUtil.toDate(ts - 1000 * 60 * 60 * 24);
                        firstViewDtState.update(yesterday);
                    }
                }
                collector.collect(jsonObject);
            }
        });

        //TODO 6. 分流
        //6.1定义启动、曝光、动作、错误侧输出流
        OutputTag<String> startTag = new OutputTag<String>("startTag"){

        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){

        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {

        };
        OutputTag<String> errorTag = new OutputTag<String>("errorTag") {

        };

        //6.2 分流
        SingleOutputStreamOperator<String> separatedStream = fixedStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //6.2.1 收集错误数据
                JSONObject error = jsonObject.getJSONObject("err");
                if (error != null) {
                    context.output(errorTag, jsonObject.toJSONString());
                }

                //剔除“error”字段
                jsonObject.remove("err");

                //6.2.2 收集启动数据
                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //获取“page”字段
                    JSONObject page = jsonObject.getJSONObject("page");
                    //获取“common”字段
                    JSONObject common = jsonObject.getJSONObject("common");
                    //获取“ts”
                    Long ts = jsonObject.getLong("ts");

                    //6.2.3收集曝光数据
                    JSONArray displays = jsonObject.getJSONArray("display");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            JSONObject displayObj = new JSONObject();
                            displayObj.put("display", display);
                            displayObj.put("common", common);
                            displayObj.put("page", page);
                            displayObj.put("ts", ts);
                            context.output(displayTag, displayObj.toJSONString());
                        }
                    }

                    //6.2.4收集动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            JSONObject actionObj = new JSONObject();
                            actionObj.put("action", action);
                            actionObj.put("common", common);
                            actionObj.put("page", page);
                            actionObj.put("ts", ts);
                            context.output(actionTag, actionObj.toJSONString());
                        }
                    }

                    //6.2.5 收集页面数据
                    jsonObject.remove("display");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });

        //打印主流和各侧输出流查看分流效果
        separatedStream.print("page>>>");
        separatedStream.getSideOutput(startTag).print("start!!!");
        separatedStream.getSideOutput(displayTag).print("display@@@");
        separatedStream.getSideOutput(actionTag).print("action###");
        separatedStream.getSideOutput(errorTag).print("error$$$");

        //TODO 7.将数据输出到kafka的不同主题
        //7.1 提取各侧输出流
        DataStream<String> startDs = separatedStream.getSideOutput(startTag);
        DataStream<String> displayDS = separatedStream.getSideOutput(displayTag);
        DataStream<String> actionDS = separatedStream.getSideOutput(actionTag);
        DataStream<String> errorDS = separatedStream.getSideOutput(errorTag);

        //7.2 定义不同日志输出到kafka的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        separatedStream.addSink(KafkaUtil.getKafkaProducer(page_topic));
        startDs.addSink(KafkaUtil.getKafkaProducer(start_topic));
        displayDS.addSink(KafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(KafkaUtil.getKafkaProducer(action_topic));
        errorDS.addSink(KafkaUtil.getKafkaProducer(error_topic));

        env.execute();
    }
}
