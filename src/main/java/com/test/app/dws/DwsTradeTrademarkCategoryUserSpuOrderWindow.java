package com.test.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.app.func.DimAsyncFunction;
import com.test.bean.TradeTrademarkCategoryUserSpuOrderBean;
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
 * 交易域品牌-品类-用户-SPU粒度下单各窗口汇总表
 *  主要任务:
 * 从 Kafka 订单明细主题读取数据，过滤 null 数据并按照唯一键对数据去重，关联维度信息，按照维度分组，统计各维度各窗口的订单数和订单金额，将数据写入 ClickHouse 交易域品牌-品类-用户-SPU粒度下单各窗口汇总表。
 *  思路分析:
 * 与上文提到的 DWS 层宽表相比，本程序新增了维度关联操作。
 * 维度表保存在 HBase，首先要在 PhoenixUtil 工具类中补充查询方法。
 * 1）PhoenixUtil 查询方法思路
 * （1）元组
 * （2）实体类
 * 2）Phoenix 维度查询
 * 3）旁路缓存优化
 * （1）旁路缓存策略应注意两点
 * a）缓存要设过期时间，不然冷数据会常驻缓存，浪费资源。
 * b）要考虑维度数据是否会发生变化，如果发生变化要主动清除缓存。
 * （2）缓存的选型
 * 一般两种：堆缓存或者独立缓存服务（memcache，redis）
 * 堆缓存，性能更好，效率更高，因为数据访问路径更短。但是难于管理，其它进程无法维护缓存中的数据。
 * 独立缓存服务（redis,memcache），会有创建连接、网络IO等消耗，较堆缓存略差，但性能尚可。独立缓存服务便于维护和扩展，对于数据会发生变化且数据量很大的场景更加适用，此处选择独立缓存服务，将 redis 作为缓存介质。
 * （3）实现步骤
 * 从缓存中获取数据。
 * ① 如果查询结果不为 null，则返回结果。
 * ② 如果缓存中获取的结果为 null，则从 Phoenix 表中查询数据。
 * a）如果结果非空则将数据写入缓存后返回结果。
 * b）否则提示用户：没有对应的维度数据
 * 	注意：缓存中的数据要设置超时时间，本程序设置为 1 天。此外，如果原表数据发生变化，要删除对应缓存。为了实现此功能，需要对维度分流程序做如下修改：
 * 	i）在 MyBroadcastFunction的 processElement 方法内将操作类型字段添加到 JSON 对象中。
 * 	ii）在 DimUtil 工具类中添加 deleteCached 方法，用于删除变更数据的缓存信息。
 * 	iii）在 MyPhoenixSink 的 invoke 方法中补充对于操作类型的判断，如果操作类型为 update 则清除缓存。
 * 5）异步 IO
 *
 * 6)模板方法设计模式
 * 模板类 DimAsyncFunction，在其中定义了维度关联的具体流程
 * 	a）根据流中对象获取维度主键。
 * 	b）根据维度主键获取维度对象。
 * 	c）用上一步的查询结果补全流中对象的维度信息
 *
 * 	7)执行步骤
 * 	（1）从 Kafka 订单明细主题读取数据
 * 	（2）过滤 null 数据并转换数据结构
 * 	（3）按照唯一键去重
 * 	（4）转换数据结构
 * 	JSONObject 转换为实体类 TradeTrademarkCategoryUserSpuOrderBean。
 * 	（5）补充维度信息
 * 	① 关联 sku_info 表
 * 	获取 tm_id，category3_id，spu_id。
 * 	② 关联 spu_info 表
 * 	获取 spu_name。
 * 	③ 关联 base_trademark 表
 * 	获取 tm_name。
 * 	④ 关联 base_category3 表
 * 	获取 name（三级品类名称），获取 category2_id。
 * 	⑤ 关联 base_categroy2 表
 * 	获取 name（二级品类名称），category1_id。
 * 	⑥ 关联 base_category1 表
 * 	获取 name（一级品类名称）。
 * （6）设置水位线
 * （7）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * （8）写出到 ClickHouse。
 *
 */
public class DwsTradeTrademarkCategoryUserSpuOrderWindow {

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

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取下单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        //TODO 4. 过滤null数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDs = source.filter(
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

        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDs.map(JSON::parseObject);

        //TODO 5. 按照order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        //TODO 6.去重
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
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObject.getString("wor_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if(lastValue != null){
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );

        //TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> javaBeanStream = processedStream.map(
                jsonObject -> {
                    String orderId = jsonObject.getString("order_id");
                    String userId = jsonObject.getString("user_id");
                    String skuId = jsonObject.getString("sku_id");
                    Double splitTotalAmount = jsonObject.getDouble("split_total_amount");
                    Long ts = jsonObject.getLong("ts");
                    TradeTrademarkCategoryUserSpuOrderBean tradeTrademarkCategoryUserSpuOrderBean = TradeTrademarkCategoryUserSpuOrderBean.builder()
                            .orderCount(1L)
                            .userId(userId)
                            .skuId(skuId)
                            .orderAmount(splitTotalAmount)
                            .ts(ts)
                            .build();
                    return tradeTrademarkCategoryUserSpuOrderBean;
                }
        );

        //TODO 8. 维度关联
        //8.1 关联sku_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuInfoStream = AsyncDataStream.unorderedWait(
                javaBeanStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_sku_info".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject JsonObj) throws Exception {
                        javaBean.setTrademarkId(JsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(JsonObj.getString("category3_id".toUpperCase()));
                        javaBean.setSpuId(JsonObj.getString("spu_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {

                        return javaBean.getSkuId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // 8.2 关联spu_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuInfoStream = AsyncDataStream.unorderedWait(

                withSkuInfoStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>(
                        "dim_spu_info".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setSpuName(
                                dimJsonObj.getString("spu_name".toUpperCase())
                        );

                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getSpuId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        //8.1 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                withSpuInfoStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>(
                        "dim_base_trademark".toUpperCase()
                ) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkName(jsonObj.getString("tm_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60,
                TimeUnit.SECONDS

        );

        // 8.4 关联三级分类表base_category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>(
                        "dim_base_category3".toUpperCase()
                ) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setCategory3Name(dimJsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(dimJsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60
                , TimeUnit.SECONDS
        );

        // 8.5 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>(
                        "dim_base_category2".toUpperCase()
                ) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setCategory2Name(dimJsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(dimJsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        );

        //8.6 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>(
                        "dim_base_category1".toUpperCase()
                ) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setCategory1Name(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        );

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withWatermarksDS = withCategory1Stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeTrademarkCategoryUserSpuOrderBean>forMonotonousTimestamps().withTimestampAssigner(
                        /**
                         *     new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {
                         *                             @Override
                         *                             public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean javaBean, long l) {
                         *                                 return javaBean.getTs() * 1000;
                         *                             }
                         *                         }
                         */
                        (SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>) (javaBean, l) -> javaBean.getTs() * 1000
                )
        );

        //TODO 10. 分组
        KeyedStream<TradeTrademarkCategoryUserSpuOrderBean, String> keyedForAggregateStream = withWatermarksDS.keyBy(
                new KeySelector<TradeTrademarkCategoryUserSpuOrderBean, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) throws Exception {

                        return javaBean.getTrademarkId() +
                                javaBean.getTrademarkName() +
                                javaBean.getCategory1Id() +
                                javaBean.getCategory1Name() +
                                javaBean.getCategory2Name() +
                                javaBean.getCategory3Id() +
                                javaBean.getCategory3Name() +
                                javaBean.getUserId() +
                                javaBean.getSpuId() +
                                javaBean.getSpuName();
                    }
                }
        );

        //TODO 11. 开窗
        WindowedStream<TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow> windowDs = keyedForAggregateStream.window(
                TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)
                )
        );

        //TODO 12. 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reducedStream = windowDs.reduce(
                new ReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>.Context context, Iterable<TradeTrademarkCategoryUserSpuOrderBean> iterable, Collector<TradeTrademarkCategoryUserSpuOrderBean> collector) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TradeTrademarkCategoryUserSpuOrderBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            collector.collect(element);
                        }
                    }
                }
        );

        //TODO 13. 写出到OLAP数据库
        SinkFunction<TradeTrademarkCategoryUserSpuOrderBean> jdbcSink = ClickHouseUtil.getJdbcSink(
                "insert into dws_trade_trademark_category_user_spu_order_window values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        reducedStream.<TradeTrademarkCategoryUserSpuOrderBean>addSink(jdbcSink);

        env.execute();

    }

}
