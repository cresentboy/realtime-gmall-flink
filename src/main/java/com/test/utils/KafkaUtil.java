package com.test.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * 和 Kafka 交互要用到 Flink 提供的 FlinkKafkaConsumer、FlinkKafkaProducer 类，为了提高模板代码的复用性，将其封装到 KafkaUtil 工具类中。
 */
public class KafkaUtil {
    static String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    static String DEFAULT_TOPIC = "default_topic";




    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }

            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord != null && consumerRecord.value() != null) {
                    return new String(consumerRecord.value());
                }
                return null;
            }
        },properties);
        return flinkKafkaConsumer;
    }

public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 15 * 1000 + "");
    FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long aLong) {
            return new ProducerRecord<>(topic, jsonStr.getBytes());
        }
    }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    return producer;
}

    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */

public static String getKafkaDDL(String topic, String groupId){

    return " with ('connector' = 'kafka', " +
            " 'topic' = '" + topic + "'," +
            " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
            " 'properties.group.id' = '" + groupId + "', " +
            " 'format' = 'json', " +
            " 'scan.startup.mode' = 'group-offsets')";
}

public static String getUpsertKafkaDDL(String topic){

    return "WITH ( " +
            "  'connector' = 'upsert-kafka', " +
            "  'topic' = '" + topic + "', " +
            "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
            "  'key.format' = 'json', " +
            "  'value.format' = 'json' " +
            ")";

}
}
