package com.test.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.bean.TrafficHomeDetailPageViewBean;
import com.test.bean.TrafficPageViewBean;
import com.test.utils.ClickHouseUtil;
import com.test.utils.DateFormatUtil;
import com.test.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 流量域页面浏览各窗口汇总表
 *  主要任务:
 * 从 Kafka 页面日志主题读取数据，统计当日的首页和商品详情页独立访客数。
 *  思路分析:
 * 1）读取 Kafka 页面主题数据
 * 2）转换数据结构
 * 将流中数据由 String 转换为 JSONObject。
 * 3）过滤数据
 * 	仅保留 page_id 为 home 或 good_detail 的数据，因为本程序统计的度量仅与这两个页面有关，其它数据无用。
 * 4）设置水位线
 * 5）按照 mid 分组
 * 6）统计首页和商品详情页独立访客数，转换数据结构
 * 运用 Flink 状态编程，为每个 mid 维护首页和商品详情页末次访问日期。如果 page_id 为 home，当状态中存储的日期为 null 或不是当日时，将 homeUvCt（首页独立访客数） 置为 1，并将状态中的日期更新为当日。否则置为 0，不做操作。商品详情页独立访客的统计同理。当 homeUvCt 和 detailUvCt 至少有一个不为 0 时，将统计结果和相关维度信息封装到定义的实体类中，发送到下游，否则舍弃数据。
 * 7）开窗
 * 8）聚合
 * 9）将数据写出到 ClickHouse
 *
 */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3, Time.days(1), Time.minutes(1)
                )
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 读取 Kafka dwd_traffic_page_log 数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构 String -> JSONObject
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        //TODO 5. 过滤page_id 不为home && page_id 不为good_detail的数据
        SingleOutputStreamOperator<JSONObject> filteredStream = mappedStream.filter(
                jsonObject -> {
                    JSONObject page = jsonObject.getJSONObject("page");
                    String pageId = page.getString("page_Id");
                    return pageId.equals("home") || pageId.equals("good_detail");
                }
        );

        //TODO 6. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filteredStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );

        //TODO 7. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkDS.keyBy(
                r -> r.getJSONObject("common").getString("mid")
        );

        //TODO 8. 鉴别独立访客，转换数据结构
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    private ValueState<String> homeLastVisitDt;
                    private ValueState<String> detailLastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        homeLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("home_last_visit_dt", String.class)
                        );
                        detailLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("detail_last_visit_dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {

                        String homeLastDt = homeLastVisitDt.value();
                        String detailLastDt = detailLastVisitDt.value();
                        JSONObject page = jsonObject.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        Long ts = jsonObject.getLong("ts");
                        String visitDt = DateFormatUtil.toDate(ts);
                        long homeUvCt = 0L;
                        long detailUvCt = 0L;

                        if (pageId.equals("home")) {
                            if (homeLastDt == null || !homeLastDt.equals(visitDt)) {
                                homeUvCt = 1L;
                                homeLastVisitDt.update(visitDt);
                            }
                        }

                        if (pageId.equals("good_detail")) {
                            if (detailLastDt == null || !detailLastDt.equals(visitDt)) {
                                detailUvCt = 1L;
                                detailLastVisitDt.update(visitDt);
                            }
                        }

                        if (homeUvCt != 0 || detailUvCt != 0) {
                            collector.collect(
                                    new TrafficHomeDetailPageViewBean(
                                            "",
                                            "",
                                            homeUvCt,
                                            detailUvCt,
                                            0L
                                    )
                            );
                        }
                    }
                }
        );

        //TODO 9. 开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowStream = uvStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
        ));

        //TODO 10. 聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reducedStream = windowStream.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        return value1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(timeWindow.getStart());
                        String edt = DateFormatUtil.toYmdHms(timeWindow.getEnd());
                        for (TrafficHomeDetailPageViewBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setTs(System.currentTimeMillis());
                            collector.collect(value);
                        }

                    }
                }
        );

        //TODO 11. 写出到OLAP数据库
        SinkFunction<TrafficHomeDetailPageViewBean> jdbcSink = ClickHouseUtil.<TrafficHomeDetailPageViewBean>getJdbcSink(
                "insert into dws_traffic_page_view_window values(?,?,?,?,?)"
        );
        reducedStream.<TrafficHomeDetailPageViewBean>addSink(jdbcSink);
    }
}
