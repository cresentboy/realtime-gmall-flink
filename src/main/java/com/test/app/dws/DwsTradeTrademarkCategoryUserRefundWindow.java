package com.test.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.app.func.DimAsyncFunction;
import com.test.bean.TradeTrademarkCategoryUserRefundBean;
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
import org.apache.flink.api.java.functions.KeySelector;
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

import java.util.concurrent.TimeUnit;

/**
 * 交易域品牌-品类-用户粒度退单各窗口汇总表
 * 主要任务:
 * 	从 Kafka 读取退单明细数据，过滤 null 数据并按照唯一键对数据去重，关联维度信息，按照维度分组，统计各维度各窗口的订单数，将数据写入 ClickHouse 交易域品牌-品类-用户粒度退单各窗口汇总表。
 *  思路分析:
 * 1）从 Kafka 退单明细主题读取数据
 * 2）过滤 null 数据并转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * 	JSONObject 转换为实体类 TradeTrademarkCategoryUserRefundBean。
 * 5）补充维度信息
 * 	（1）关联 sku_info 表
 * 	获取 tm_id，category3_id。
 * 	（2）关联 base_trademark 表
 * 	获取 tm_name。
 * 	（3）关联 base_category3 表
 * 	获取 name（三级品类名称），获取 category2_id。
 * 	（4）关联 base_categroy2 表
 * 	获取 name（二级品类名称），category1_id。
 * （5）关联 base_category1 表
 * 	获取 name（一级品类名称）。
 * 6）设置水位线
 * 7）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * 8）写出到 ClickHouse。
 *
 *
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
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
                        3, Time.days(1L), Time.minutes(1L)
                )
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_trade_order_refund 主题读取退单明细数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 过滤 null 数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String provinceId = jsonObj.getString("province_id");
                            if (provinceId != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        //TODO 5. 按照order_refund_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        //TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastValuestate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValuestate = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>(
                                        "last_value_state",
                                        JSONObject.class
                                )
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject lastValue = lastValuestate.value();
                        if (lastValue == null) {
                            context.timerService().registerProcessingTimeTimer(5000L);
                            lastValuestate.update(jsonObject);
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObject.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(
                                    lastRowOpTs, rowOpTs
                            ) <= 0
                            ) {
                                lastValuestate.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        JSONObject lastValue = lastValuestate.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValuestate.clear();
                    }
                }
        );

        //TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> javaBeanStream = processedStream.map(
                jsonObject -> {
                    String orderId = jsonObject.getString("order_id");
                    String skuId = jsonObject.getString("sku_id");
                    String userId = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");

                    TradeTrademarkCategoryUserRefundBean trademarkCategoryUserOrderBean = TradeTrademarkCategoryUserRefundBean.builder()
                            .refundCount(1L)
                            .userId(userId)
                            .skuId(skuId)
                            .ts(ts)
                            .build();
                    return trademarkCategoryUserOrderBean;
                }
        );

        //TODO 8. 维度关联
        //8.1 关联sku_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withSkuInforStream = AsyncDataStream.unorderedWait(
                javaBeanStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>(
                        "dim_sku_info".toUpperCase()
                ) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setTrademarkId(dimJsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(dimJsonObj.getString("category3_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getSkuId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        //8.2 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                withSkuInforStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>(
                        "dim_base_trademark".toUpperCase()
                ) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setTrademarkName(dimJsonObj.getString("ts_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        );

        // 8.3 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>(
                        "dim_base_category3".toUpperCase()
                ) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setCategory3Name(dimJsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(dimJsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {

                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        );

        //8.4 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category2".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 8.5 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withWatermarkDS = withCategory1Stream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserRefundBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean javaBean, long recordTimestamp) {
                                        return javaBean.getTs() * 1000;
                                    }
                                }
                        )
        );

        // TODO 10. 分组
        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> keyedForAggregateStream = withWatermarkDS.keyBy(
                new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) throws Exception {
                        return javaBean.getTrademarkId() +
                                javaBean.getTrademarkName() +
                                javaBean.getCategory1Id() +
                                javaBean.getCategory1Name() +
                                javaBean.getCategory2Id() +
                                javaBean.getCategory2Name() +
                                javaBean.getCategory3Id() +
                                javaBean.getCategory3Name() +
                                javaBean.getUserId();
                    }
                }
        );

        // TODO 11. 开窗
        WindowedStream<TradeTrademarkCategoryUserRefundBean, String, TimeWindow> windowDS = keyedForAggregateStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 12. 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reducedStream = windowDS.reduce(
                new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>.Context context, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TradeTrademarkCategoryUserRefundBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            collector.collect(element);
                        }
                    }
                }
        );

        // TODO 13. 写出到 OLAP 数据库
        SinkFunction<TradeTrademarkCategoryUserRefundBean> jdbcSink =
                ClickHouseUtil.getJdbcSink(
                        "insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
        reducedStream.addSink(jdbcSink);

        env.execute();

    }
}
