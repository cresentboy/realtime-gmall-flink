package com.test.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.bean.TradePaymentWindowBean;
import com.test.utils.ClickHouseUtil;
import com.test.utils.DateFormatUtil;
import com.test.utils.KafkaUtil;
import com.test.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 交易域支付各窗口汇总表
 *  主要任务:
 * 从 Kafka 读取交易域支付成功主题数据，统计支付成功独立用户数和首次支付成功用户数。
 *  思路分析:
 * 1）从 Kafka 支付成功明细主题读取数据
 * 2）过滤为 null 的数据，转换数据结构
 * 	String 转换为 JSONObject。
 * 3）按照唯一键分组
 * 4）去重
 * 	与前文同理。
 * 5）设置水位线，按照 user_id 分组
 * 6）统计独立支付人数和新增支付人数
 * 	运用 Flink 状态编程，在状态中维护用户末次支付日期。
 * 	若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；否则首次支付用户数置为 0，判断末次支付日期是否为当日，如果不是当日则支付独立用户数置为 1，否则置为 0。最后将状态中的支付日期更新为当日。
 * 7）开窗、聚合
 * 度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * 8）写出到 ClickHouse
 *
 *
 *
 */
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {

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
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_trade_pay_detail_suc 主题读取支付成功明细数据，封装为流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        //TODO 4.过滤null数据，转化为数据结构
        SingleOutputStreamOperator<String> filteredStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String provinceId = jsonObj.getString("province_id");
                            String paymentTypeCode = jsonObj.getString("payment_type_code");
                            String paymentTypeName = jsonObj.getString("payment_type_name");
                            String sourceTypeName = jsonObj.getString("source_type_name");
                            Long ts = jsonObj.getLong("ts");

                            if (provinceId != null && paymentTypeCode != null && ts != null && paymentTypeName != null && sourceTypeName != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );

        SingleOutputStreamOperator<JSONObject> mappedStream = filteredStream.map(JSON::parseObject);

        //TODO 5. 按照唯一键order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        //TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastDataState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>(
                                        "last_data_state",
                                        JSONObject.class
                                )
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject lastData = lastDataState.value();
                        if (lastData == null) {
                            context.timerService().registerProcessingTimeTimer(5000L);
                            lastDataState.update(jsonObject);
                        } else {
                            String lastRowOpTs = lastData.getString("row_op_ts");
                            String rowOpTs = jsonObject.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(
                                    lastRowOpTs, rowOpTs
                            ) <= 0
                            ) {
                                lastDataState.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        JSONObject value = lastDataState.value();
                        if (value != null) {
                            out.collect(value);
                        }
                        lastDataState.clear();
                    }
                }
        );

        //TODO 7. 设置水位线
        /**
         *  经过两次 keyedProcessFunction 处理之后开窗，数据的时间语义会发生紊乱，可能会导致数据无法进入正确的窗口
         *         因此使用处理时间去重，在分组统计之前设置一次水位线
         */
        SingleOutputStreamOperator<JSONObject> withWatermarkSecondStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                                return jsonObject.getLong("ts") * 1000;
                            }
                        }
                )
        );

        //TODO 8. 按照用户id分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkSecondStream.keyBy(
                r -> r.getString("user_id")
        );

        //TODO 9. 统计独立支付人数和新增支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> paymentWindowBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {
                    private ValueState<String> lastPaySucDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastPaySucDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>(
                                        "last_pay_suc_dt_state",
                                        String.class
                                )
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>.Context context, Collector<TradePaymentWindowBean> collector) throws Exception {
                        String lastPaySucDt = lastPaySucDtState.value();
                        Long ts = jsonObject.getLong("ts");
                        String paySucDt = DateFormatUtil.toDate(ts);
                        Long paymentSucUniqueUserCount = 0L;
                        Long paymentSucNewUserCount = 0L;

                        if (lastPaySucDt == null) {
                            paymentSucUniqueUserCount = 1L;
                            paymentSucNewUserCount = 1L;
                        } else {
                            if (!lastPaySucDt.equals(paySucDt)) {
                                paymentSucUniqueUserCount = 1L;
                            }
                        }
                        lastPaySucDtState.update(paySucDt);

                        TradePaymentWindowBean tradePaymentWindowBean = new TradePaymentWindowBean(
                                "",
                                "",
                                paymentSucUniqueUserCount,
                                paymentSucNewUserCount,
                                ts
                        );

                        long currentWatermark = context.timerService().currentWatermark();
                        collector.collect(tradePaymentWindowBean);
                    }
                }
        );

        //TODO 10. 开窗
        AllWindowedStream<TradePaymentWindowBean, TimeWindow> windowDS = paymentWindowBeanStream.windowAll(
                TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
                )
        );

        //TODO 11. 聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> aggregatedDS = windowDS.aggregate(
                new AggregateFunction<TradePaymentWindowBean, TradePaymentWindowBean, TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean createAccumulator() {
                        return new TradePaymentWindowBean(
                                "",
                                "",
                                0L,
                                0L,
                                0L
                        );
                    }

                    @Override
                    public TradePaymentWindowBean add(TradePaymentWindowBean value, TradePaymentWindowBean accumulator) {
                        accumulator.setPaymentSucUniqueUserCount(
                                accumulator.getPaymentSucUniqueUserCount() + value.getPaymentSucUniqueUserCount()
                        );
                        accumulator.setPaymentSucNewUserCount(
                                accumulator.getPaymentSucNewUserCount() + value.getPaymentSucNewUserCount()
                        );

                        return accumulator;
                    }

                    @Override
                    public TradePaymentWindowBean getResult(TradePaymentWindowBean accumulator) {

                        return accumulator;
                    }

                    @Override
                    public TradePaymentWindowBean merge(TradePaymentWindowBean a, TradePaymentWindowBean b) {
                        return null;
                    }
                },
                new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>.Context context, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TradePaymentWindowBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            collector.collect(element);
                        }
                    }
                }
        );

        //TODO 12. 写出到OLAP数据库(clickhouse)
        SinkFunction<TradePaymentWindowBean> jdbcSink = ClickHouseUtil.getJdbcSink(
                "insert into dws_trade_payment_suc_window values(?,?,?,?,?)"
        );
        aggregatedDS.addSink(jdbcSink);
        env.execute();


    }
}
