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
        /**
         * 对流中数据进行解析，将字符串转换为 JSONObject，如果解析报错则必然为脏数据。
         * 定义侧输出流，将脏数据发送到侧输出流，写入 Kafka 脏数据主题
         */
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
        /**
         * 新老访客状态标记修复思路
         * 运用 Flink 状态编程，为每个 mid 维护一个键控状态，记录首次访问日期
         * ①如果 is_new 的值为 1
         * a）如果键控状态为 null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
         * b）如果键控状态不为 null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
         * c）如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；
         *
         * ②如果 is_new 的值为 0
         * a）如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。
         * 当前端新老访客状态标记丢失时，日志进入程序被判定为老访客，Flink 程序就可以纠正被误判的访客状态标记，
         * 只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
         * b）如果键控状态不为 null，说明程序已经维护了首次访问日期，不做操作。
         */
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
        /**
         * 利用侧输出流实现数据拆分
         * （1）埋点日志结构分析
         * 前端埋点获取的 JSON 字符串（日志）可能存在 common、start、page、displays、actions、err 六种字段。其中
         * 	common 对应的是公共信息，是所有日志都有的字段
         * 	err 对应的是错误信息，所有日志都可能有的字段
         * 	start 对应的是启动信息，启动日志才有的字段
         * 	page 对应的是页面信息，页面日志才有的字段
         * 	displays 对应的是曝光信息，曝光日志才有的字段，曝光日志可以归为页面日志，因此必然有 page 字段
         * 	actions 对应的是动作信息，动作日志才有的字段，同样属于页面日志，必然有 page 字段。动作信息和曝光信息可以同时存在。
         * 	ts 对应的是时间戳，单位：毫秒，所有日志都有的字段
         * 综上，我们可以将前端埋点获取的日志分为两大类：启动日志和页面日志。二者都有 common 字段和 ts 字段，都可能有 err 字段。
         * 页面日志一定有 page 字段，一定没有 start 字段，可能有 displays 和 actions 字段；
         * 启动日志一定有 start 字段，一定没有 page、displays 和 actions 字段。
         *
         * （2）分流日志分类
         * 本节将按照内容，将日志分为以下五类
         * 	启动日志
         * 	页面日志
         * 	曝光日志
         * 	动作日志
         * 	错误日志
         *
         * （3）分流思路
         * ①所有日志数据都可能拥有 err 字段，所以首先获取 err 字段，如果返回值不为 null 则将整条日志数据发送到错误侧输出流。然后删掉 JSONObject 中的 err 字段及对应值；
         * ②判断是否有 start 字段，如果有则说明数据为启动日志，将其发送到启动侧输出流；如果没有则说明为页面日志，进行下一步；
         * ③页面日志必然有 page 字段、 common 字段和 ts 字段，获取它们的值，ts 封装为包装类 Long，其余两个字段的值封装为 JSONObject；
         * ④判断是否有 displays 字段，如果有，将其值封装为 JSONArray，遍历该数组，依次获取每个元素（记为 display），封装为JSONObject。
         * 创建一个空的 JSONObject，将 display、common、page和 ts 添加到该对象中，获得处理好的曝光数据，发送到曝光侧输出流。
         * 动作日志的处理与曝光日志相同（注意：一条页面日志可能既有曝光数据又有动作数据，二者没有任何关系，因此曝光数据不为 null 时仍要对动作数据进行处理）；
         * ⑤动作日志和曝光日志处理结束后删除 displays 和 actions 字段，此时主流的 JSONObject 中只有 common 字段、 page 字段和 ts 字段，即为最终的页面日志。
         * 	处理结束后，页面日志数据位于主流，其余四种日志分别位于对应的侧输出流，将五条流的数据写入 Kafka 对应主题即可。
         */
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
