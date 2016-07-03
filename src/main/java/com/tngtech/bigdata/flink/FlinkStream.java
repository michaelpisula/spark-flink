package com.tngtech.bigdata.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public abstract class FlinkStream {

    private static final String KAFKA_BROKERS     = "10.100.0.13:9092";
    private static final String ZOOKEEPER_CONNECT = "10.100.013:2181";
    private static final String CONSUMER_GROUP    = "<replace_me>";

    public static void main(final String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Example Kafka Source
        final DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer082<>("someTopic",
                new SimpleStringSchema(), getKafkaProperties()));

//        final finalDataStreamSource<String> kafkaSource = env.readTextFile("sales.csv"); // Comment in for testing without kafka

        kafkaSource.map(line -> line.split(","))
                   .map(new MapFunction<String[], Tuple3<Integer, String, Integer>>() {
                       @Override
                       public Tuple3<Integer, String, Integer> map(final String[] parts) throws Exception {
                           final int id = Integer.parseInt(parts[0]);
                           final int amount = Integer.parseInt(parts[1]);
                           final String date = parts[2];
                           return new Tuple3<>(id, date, amount);
                       }
                   })
                   .keyBy(0, 1)
                   .sum(2)
                   .print();

        env.execute(); //Start processing
    }


    private static Properties getKafkaProperties() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKERS);
        properties.put("zookeeper.connect", ZOOKEEPER_CONNECT);
        properties.put("group.id", CONSUMER_GROUP);
        return properties;
    }
}
