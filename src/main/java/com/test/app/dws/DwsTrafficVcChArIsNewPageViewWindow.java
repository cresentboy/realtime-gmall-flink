package com.test.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.bean.TrafficPageViewBean;
import com.test.utils.ClickHouseUtil;
import com.test.utils.DateFormatUtil;
import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 *  主要任务:
 * DWS 层是为 ADS 层服务的，通过对指标体系的分析，本节汇总表中需要有会话数、页面浏览数、浏览总时长、独立访客数、跳出会话数五个度量字段。本节的任务是统计这五个指标，并将维度和度量数据写入 ClickHouse 汇总表。
 *  思路分析:
 *  任务可以分为两部分：统计指标的计算和数据写出
 *  执行步骤
 * （1）读取页面主题数据，封装为流
 * （2）统计页面浏览时长、页面浏览数、会话数，转换数据结构
 * 创建实体类，将独立访客数、跳出会话数置为 0，将页面浏览数置为 1（只要有一条页面浏览日志，则页面浏览数加一），获取日志中的页面浏览时长，赋值给实体类的同名字段，最后判断 last_page_id 是否为 null，如果是，说明页面是首页，开启了一个新的会话，将会话数置为 1，否则置为 0。补充维度字段，窗口起始和结束时间置为空字符串。下游要根据水位线开窗，所以要补充事件时间字段，此处将日志生成时间 ts 作为事件时间字段即可。最后将实体类对象发往下游。
 * （3）读取用户跳出明细数据
 * （4）转换用户跳出流数据结构
 * 封装实体类，维度字段和时间戳处理与页面流相同，跳出数置为1，其余度量字段置为 0。将数据发往下游。
 * （5）读取独立访客明细数据
 * （6）转换独立访客流数据结构
 * 处理过程与跳出流同理。
 * （7）union 合并三条流
 * （8）设置水位线；
 * （9）按照维度字段分组；
 * （10）开窗
 * （11）聚合计算
 * （12）将数据写入 ClickHouse。
 *
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3. 从kafka dwd_traffic_page_log 主题读取页面数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_channel_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        //TODO 4.转换页面流数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageLogSource.map(JSON::parseObject);

        //TODO 5. 统计会话数、页面浏览数、页面访问时长，封装为实体类
        SingleOutputStreamOperator<TrafficPageViewBean> mainStream = jsonObjStream.map(
                /**
                 * Anonymous new MapFUnction<JSONObject,TrafficPageViewBean>() can be replaced with lambda:
                 *
                 *   new MapFunction<JSONObject, TrafficPageViewBean>() {
                 *                     @Override
                 *                     public TrafficPageViewBean map(JSONObject jsonObject) throws Exception {
                 */
                (MapFunction<JSONObject, TrafficPageViewBean>) jsonObject -> {
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");

                    //获取ts
                    Long ts = jsonObject.getLong("ts");

                    //获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    //获取页面访问时长
                    Long duringTime = page.getLong("during_time");

                    //定义变量接收其他度量值
                    Long uvCt = 0L;
                    Long svCt = 0L;
                    Long pvCt = 1L;
                    Long ujCt = 0L;

                    //判断本页面是否开启了一个新会话
                    String lastPageId = page.getString("last_page_id");
                    if (lastPageId == null) {
                        svCt = 1L;
                    }
                    //封装为实体类
                    TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            uvCt,
                            svCt,
                            pvCt,
                            duringTime,
                            ujCt,
                            ts
                    );
                    return trafficPageViewBean;
                }
        );

        //TODO 6. 从kafka读取跳出明细数据和独立访客数据，封装为流并转换数据结构，合并为三条流
        //6.1 从kafka dwd_traffic_user_jump_detail读取跳出明细数据，封装为流
        String ujdTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> ujdKafkaConsumer = KafkaUtil.getKafkaConsumer(ujdTopic, groupId);
        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> ujdMaoppedStream = ujdSource.map(jsonStr -> {
            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            JSONObject common = jsonObject.getJSONObject("common");
            long ts = jsonObject.getLong("ts") + 10 * 1000L;

            //获取维度信息
            String vc = common.getString("vc");
            String ch = common.getString("ch");
            String ar = common.getString("ar");
            String isNew = common.getString("is_new");

            //封装为实体类
            return new TrafficPageViewBean(
                    "",
                    "",
                    vc,
                    ch,
                    ar,
                    isNew,
                    0L,
                    0L,
                    0L,
                    0L,
                    1L,
                    ts
            );
        });

        // 6.2 从 Kafka dwd_traffic_unique_visitor_detail 主题读取独立访客数据，封装为流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> uvKafkaConsumer = KafkaUtil.getKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> uvMappedStream =
                uvSource.map(jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            ts
                    );
                });

        //TODO 6.3 合并三条流
        DataStream<TrafficPageViewBean> pageViewBeanDS = mainStream.union(ujdMaoppedStream)
                .union(uvMappedStream);

        //TODO 7. 设置水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream = pageViewBeanDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps().withTimestampAssigner(
                        /**
                         * Anonymous new SerializableTimestampAssigner<TrafficPageViewBean>() can be replaced with lambda :
                         *
                         *      new SerializableTimestampAssigner<TrafficPageViewBean>() {
                         *                             @Override
                         *                             public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                         *                                 return trafficPageViewBean.getTs();
                         *                             }
                         *                         }
                         */
                        (SerializableTimestampAssigner<TrafficPageViewBean>) (trafficPageViewBean, l) -> trafficPageViewBean.getTs()
                ));

        //TODO 8.按照维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedBeanStream = withWatermarkStream.keyBy(trafficPageViewBean -> Tuple4.of(
                        trafficPageViewBean.getVc(),
                        trafficPageViewBean.getCh(),
                        trafficPageViewBean.getAr(),
                        trafficPageViewBean.getIsNew()
                ),
                Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
        );

        //TODO 9. 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedBeanStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
        )).allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L));

        //TODO 10. 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reducedStream = windowStream.reduce(
                /**
                 * new ReduceFunction<TrafficPageViewBean>() {
                 *                     @Override
                 *                     public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                 *                         value1.setUvCt(value1.getUjCt() + value2.getUjCt());
                 *                         value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                 *                         value1.setPvCt(value1.getSvCt() + value2.getPvCt());
                 *                         value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                 *                         value1.setUjCt(value1.getUvCt() + value2.getUjCt());
                 *                         return value1;
                 *                     }
                 *                 },
                 */
                (ReduceFunction<TrafficPageViewBean>) (value1, value2) -> {
                    value1.setUvCt(value1.getUjCt() + value2.getUjCt());
                    value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                    value1.setPvCt(value1.getSvCt() + value2.getPvCt());
                    value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                    value1.setUjCt(value1.getUvCt() + value2.getUjCt());
                    return value1;
                },
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> collector) {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TrafficPageViewBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            collector.collect(element);
                        }
                    }
                }
        );

        //TODO 11. 写入OLAP数据库
        reducedStream.addSink(ClickHouseUtil.getJdbcSink(
                "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));
        env.execute();
    }
}
