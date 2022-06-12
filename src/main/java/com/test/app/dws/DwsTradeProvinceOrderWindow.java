package com.test.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.app.func.DimAsyncFunction;
import com.test.bean.TradeProvinceOrderWindow;
import com.test.utils.ClickHouseUtil;
import com.test.utils.DateFormatUtil;
import com.test.utils.KafkaUtil;
import com.test.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 交易域省份粒度下单各窗口汇总表
 *  主要任务
 * 从 Kafka 读取订单明细数据，过滤 null 数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，将数据写入 ClickHouse 交易域省份粒度下单各窗口汇总表。
 *  思路分析
 * 1）从 Kafka 订单明细主题读取数据
 * 2）过滤 null 数据并转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * 	JSONObject 转换为实体类 TradeProvinceOrderWindow。
 * 5）设置水位线
 * 6）按照省份 ID 和省份名称分组
 * 此时的 provinceName 均为空字符串，但 provinceId 已经可以唯一标识省份，因此不会影响计算结果。本程序将省份名称字段的补全放在聚合之后，聚合后的数据量显著减少，这样做可以大大节省资源开销，提升性能。
 * 7）开窗
 * 8）聚合计算
 * 度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * 9）关联省份信息
 * 补全省份名称字段。
 * 10）写出到 ClickHouse。
 *
 *
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration()
        );
        env.setParallelism(4);

        //TODO 4. 状态后端设置
        env.enableCheckpointing(3000L,
                CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
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

        //TODO 3. 从kafka dwd_trade_order_detail主题读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        //TODO 4. 过滤null数据并转化数据结构
        SingleOutputStreamOperator<String> filteredDs = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String userId = jsonObj.getString("user_id");
                            String sourceTyperName = jsonObj.getString("source_type_name");
                            if (userId != null && sourceTyperName != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDs.map(JSON::parseObject);

        //TODO 5.按照order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        //TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            context.timerService().registerProcessingTimeTimer(5000L);
                            lastValueState.update(jsonObject);
                        } else {
                            String lastRowOpTs = lastValue.getString("wor_op_ts");
                            String rowOpTs = jsonObject.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );

        //TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeProvinceOrderWindow> javaBeanStream = processedStream.map(
                jsonObject -> {
                    String provinceId = jsonObject.getString("province_id");
                    String orderId = jsonObject.getString("order_id");
                    Double orderAmount = jsonObject.getDouble("split_total_amount");
                    Long ts = jsonObject.getLong("ts");

                    TradeProvinceOrderWindow tradeProvinceOrderWindow = TradeProvinceOrderWindow.builder()
                            .provinceId(provinceId)
                            .orderIdSet(new HashSet<String>(
                                    Collections.singleton(orderId)
                            ))
                            .orderAmount(orderAmount)
                            .ts(ts)
                            .build();
                    String provinceName = tradeProvinceOrderWindow.getProvinceName();
                    return tradeProvinceOrderWindow;
                }
        );

        //TODO 8. 设置水位线
        SingleOutputStreamOperator<TradeProvinceOrderWindow> withWatermarksStream = javaBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeProvinceOrderWindow>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                            @Override
                            public long extractTimestamp(TradeProvinceOrderWindow javaBean, long redordTimestamp) {
                                return javaBean.getTs() * 1000L;
                            }
                        }
                )
        );

        //TODO 9. 按照省份id和省份名称分组
        /**
         *  此时的 provinceName 均为空字符串，但 provinceId 已经可以唯一标识省份
         *  因此不会影响计算结果。本程序将省份名称字段的补全放在聚合之后
         *  聚合后的数据量显著减少，这样做可以大大节省资源开销，提升性能
         */
        KeyedStream<TradeProvinceOrderWindow, String> keyedByProIdStream = withWatermarksStream.keyBy(
                bean -> bean.getProvinceId() + bean.getProvinceName()
        );

        //TODO 10. 开窗
        WindowedStream<TradeProvinceOrderWindow, String, TimeWindow> windowDs = keyedByProIdStream.window(
                TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
                )
        );

        //TODO 11. 聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reducedStream = windowDs.reduce(
                new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderWindow> iterable, Collector<TradeProvinceOrderWindow> collector) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TradeProvinceOrderWindow element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            collector.collect(element);
                        }
                    }
                }
        );

        //TODO 12. 关联省份信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> fullInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeProvinceOrderWindow>(
                        "dim_base_province".toUpperCase()
                ) {
                    @Override
                    public void join(TradeProvinceOrderWindow javaBean, JSONObject dimJsonObj) throws Exception {
                        String provinceName = dimJsonObj.getString("name".toUpperCase());
                        javaBean.setProvinceName(provinceName);
                    }

                    @Override
                    public String getKey(TradeProvinceOrderWindow javaBean) {
                        return javaBean.getProvinceId();
                    }
                },
                60 * 50,
                TimeUnit.SECONDS
        );


        //TODO 13. 写入到OLAP数据库
        SinkFunction<TradeProvinceOrderWindow> jdbcSink = ClickHouseUtil.getJdbcSink(
                "insert into dws_trade_province_order_window-values(?,?,?,?,?,?.?)"
        );
        fullInfoStream.<TradeProvinceOrderWindow>addSink(jdbcSink);
        env.execute();
    }
}
