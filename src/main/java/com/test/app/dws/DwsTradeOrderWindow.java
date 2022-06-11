package com.test.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.bean.TradeOrderBean;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 交易域下单各窗口汇总表
 *  主要任务:
 * 从 Kafka 订单明细主题读取数据，过滤 null 数据并去重，统计当日下单独立用户数和新增下单用户数，封装为实体类，写入 ClickHouse。
 *  思路分析:
 *  FlinkSQL 提供了几个可以获取当前时间戳的函数:
 *  	localtimestamp：返回本地时区的当前时间戳，返回类型为 TIMESTAMP(3)。在流处理模式下会对每条记录计算一次时间。而在批处理模式下，仅在查询开始时计算一次时间，所有数据使用相同的时间。
 * 	current_timestamp：返回本地时区的当前时间戳，返回类型为 TIMESTAMP_LTZ(3)。在流处理模式下会对每条记录计算一次时间。而在批处理模式下，仅在查询开始时计算一次时间，所有数据使用相同的时间。
 * 	now()：与 current_timestamp 相同。
 * 	current_row_timestamp()：返回本地时区的当前时间戳，返回类型为 TIMESTAMP_LTZ(3)。无论在流处理模式还是批处理模式下，都会对每行数据计算一次时间。
 * 动态表属于流处理模式，所以四种函数任选其一即可。此处选择 current_row_timestamp()。
 * 去重思路
 * 	获取了数据生成时间，接下来要考虑的问题就是如何获取生成时间最晚的数据。此处提供两种思路。
 * 	（1）按照唯一键分组，开窗，在窗口闭合前比较窗口中所有数据的时间，将生成时间最晚的数据发送到下游，其它数据舍弃。
 * 	（2）按照唯一键分组，对于每一个唯一键，维护状态和定时器，当状态中数据为 null 时注册定时器，把数据维护到状态中。此后每来一条数据都比较它与状态中数据的生成时间，状态中只保留生成最晚的数据。如果两条数据生成时间相同（系统时间精度不足），则保留后进入算子的数据。因为我们的 Flink 程序并行度和 Kafka 分区数相同，可以保证数据有序，后来的数据就是最新的数据。
 * 	两种方案都可行，此处选择方案二。
 *
 * 	实现步骤
 * 	（1）从 Kafka订单明细主题读取数据
 * 	（2）过滤为 null 数据并转换数据结构
 * 	运用 filter 算子过滤为 null 的数据。
 * 	（3）按照 order_detail_id 分组
 * 	order_detail_id 为数据唯一键。
 * 	（4）对 order_detail_id 相同的数据去重
 * 	按照上文提到的方案对数据去重。
 * 	（5）设置水位线
 * 	（6）按照用户 id 分组
 * 	（7）计算度量字段的值
 * 	a）当日下单独立用户数和新增下单用户数
 * 	运用 Flink 状态编程，在状态中维护用户末次下单日期。
 * 	若末次下单日期为 null，则将首次下单用户数和下单独立用户数均置为 1；否则首次下单用户数置为 0，判断末次下单日期是否为当日，如果不是当日则下单独立用户数置为 1，否则置为 0。最后将状态中的下单日期更新为当日。
 * b）其余度量字段直接取流中数据的对应值即可。
 * 	（8）开窗、聚合
 * 	度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * 	（9）写出到 ClickHouse。
 *
 */
public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
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

        //TODO 3. 从kafka dwd_trade_order_detail 读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dwd_trade_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        //TODO 4. 过滤null数据并转化数据结构
        SingleOutputStreamOperator<String> filterDS = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            String userId = jsonObject.getString("user_id");
                            String sourceTypeName = jsonObject.getString("source_type_name");
                            if (userId != null && sourceTypeName != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );
        //转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = filterDS.map(JSON::parseObject);

        //TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        //TODO 6. 对order_detail_id相同的数据去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> filterState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        filterState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("filter_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                        JSONObject lastData = filterState.value();
                        if (lastData ==null){
                            context.timerService().registerProcessingTimeTimer(5000L);
                            filterState.update(jsonObject);
                        }else{
                            String lastRowOpTs = lastData.getString("row_op_ts");
                            String rowOpTs = jsonObject.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs,rowOpTs) <= 0){
                                filterState.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        JSONObject currentValue = filterState.value();
                        if (currentValue != null){
                            out.collect(currentValue);
                        }
                        filterState.clear();
                    }
                }
        );

        //TODO 7. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        //TODO 8. 按照用户id分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkStream.keyBy(r -> r.getString("user_id"));

        //TODO 9. 统计当日下单独立用户数和新增下单用户数
        SingleOutputStreamOperator<TradeOrderBean> orderBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {
                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastOrderDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_order_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context context, Collector<TradeOrderBean> collector) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();
                        String orderDt = jsonObject.getString("date_id");
                        long orderNewUserCount = 0L;
                        long orderUniqueUserCount = 0L;
                        Double splitActivityAmount = jsonObject.getDouble("split_activity_amount");
                        Double splitCouponAmount = jsonObject.getDouble("split_coupon_amount");
                        Double splitOriginalAmount = jsonObject.getDouble("split_original_amount");
                        Long ts = jsonObject.getLong("ts");

                        if (lastOrderDt == null) {
                            orderNewUserCount = 1L;
                            orderUniqueUserCount = 1L;
                        } else {
                            if (!lastOrderDt.equals(orderDt)) {
                                orderUniqueUserCount = 1L;
                            }
                        }

                        lastOrderDtState.update(orderDt);
                        TradeOrderBean tradeOrderBean = new TradeOrderBean(
                                "",
                                "",
                                orderUniqueUserCount,
                                orderNewUserCount,
                                splitActivityAmount,
                                splitCouponAmount,
                                splitOriginalAmount,
                                ts
                        );

                        collector.collect(tradeOrderBean);
                    }
                }
        );

        //TODO 10. 开窗
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = orderBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
        ));

        //TODO 11. 聚合
        SingleOutputStreamOperator<TradeOrderBean> aggregatedStream = windowDS.aggregate(
                new AggregateFunction<TradeOrderBean, TradeOrderBean, TradeOrderBean>() {
                    @Override
                    public TradeOrderBean createAccumulator() {
                        return new TradeOrderBean(
                                "",
                                "",
                                0L,
                                0L,
                                0.0,
                                0.0,
                                0.0,
                                0L
                        );
                    }

                    @Override
                    public TradeOrderBean add(TradeOrderBean tradeOrderBean, TradeOrderBean tradeOrderBean2) {
                        tradeOrderBean2.setOrderUniqueUserCount(
                                tradeOrderBean2.getOrderNewUserCount() + tradeOrderBean.getOrderNewUserCount()
                        );
                        tradeOrderBean2.setOrderNewUserCount(
                                tradeOrderBean.getOrderNewUserCount() + tradeOrderBean2.getOrderNewUserCount()
                        );
                        tradeOrderBean2.setOrderActivityReduceAmount(
                                tradeOrderBean.getOrderActivityReduceAmount() == null ? 0.0 : tradeOrderBean.getOrderActivityReduceAmount() + tradeOrderBean2.getOrderActivityReduceAmount()
                        );
                        tradeOrderBean2.setOrderOriginTotalAmount(
                                tradeOrderBean.getOrderOriginTotalAmount() == null ? 0.0 : tradeOrderBean.getOrderOriginTotalAmount() + tradeOrderBean2.getOrderOriginTotalAmount()
                        );
                        tradeOrderBean2.setOrderCouponReduceAmount(
                                tradeOrderBean.getOrderCouponReduceAmount() == null ? 0.0 : tradeOrderBean.getOrderCouponReduceAmount() + tradeOrderBean2.getOrderCouponReduceAmount()
                        );
                        return tradeOrderBean2;
                    }

                    @Override
                    public TradeOrderBean getResult(TradeOrderBean tradeOrderBean) {
                        return tradeOrderBean;
                    }

                    @Override
                    public TradeOrderBean merge(TradeOrderBean tradeOrderBean, TradeOrderBean acc1) {
                        return null;
                    }
                },
                new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(timeWindow.getStart());
                        String edt = DateFormatUtil.toYmdHms(timeWindow.getEnd());
                        for (TradeOrderBean tradeOrderBean : iterable) {
                            tradeOrderBean.setStt(stt);
                            tradeOrderBean.setEdt(edt);
                            tradeOrderBean.setTs(System.currentTimeMillis());
                            collector.collect(tradeOrderBean);
                        }
                    }
                }
        );

        //TODO 12. 写出到OLAP数据库
        SinkFunction<TradeOrderBean> jdbcSink = ClickHouseUtil.<TradeOrderBean>getJdbcSink(
                "insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"
        );
        aggregatedStream.<TradeOrderBean>addSink(jdbcSink);
        env.execute();
    }
}
