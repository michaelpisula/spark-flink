package com.tngtech.bigdata.spark;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tngtech.bigdata.util.Sale;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;

public class SparkStream {


    public static void main(final String[] args) {

        final SparkConf conf = new SparkConf().setAppName("Spark Demo").setMaster("local[*]");
        final JavaStreamingContext streamContext = new JavaStreamingContext(conf, Durations.seconds(1));
        streamContext.sparkContext().setLogLevel("ERROR");
        streamContext.checkpoint("build/tmp/checkpoint");

        // initialisiere kafka stream
        final String kafkaBroker = "localhost:9092";
        final HashSet<String> topics = Sets.newHashSet("sales");
        final HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBroker);
        kafkaParams.put("group.id", "sparkmpi");

        final JavaDStream<String> salesStream = KafkaUtils.createDirectStream(streamContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics)
                                                          .map(tuple -> tuple._2);
        //JavaDStream<String> salesStream = streamContext.textFileStream("streamSample");

        final JavaPairDStream<Sale, Integer> aggregatedSales = salesStream.map(line -> line.split(","))
                                                                          .filter(array -> array.length ==
                                                                                           3) // Rausfiltern ungültiger Zeilen
                                                                          .mapToPair(parts -> { // Umwandeln der Werte in einen POJO
                                                                              int id = Integer.parseInt(parts[0]), amount = Integer.parseInt(parts[1]);
                                                                              String date = parts[2];
                                                                              return new Tuple2<>(new Sale(id, date), amount);
                                                                          })
                                                                          .reduceByKey((x, y) -> x + y);

        final JavaPairRDD<Sale, Integer> initialRdd = streamContext.sparkContext()
                                                                   .parallelizePairs(Lists.newArrayList(new Tuple2<>(new Sale(0, ""), 0)));

        final Function3<Sale, Optional<Integer>, State<Integer>, Tuple2<Sale, Integer>> mappingFunc =
                (sale, one, state) -> {
                    final int sum = one.or(0) + (state.exists() ? state.get() : 0);
                    final Tuple2<Sale, Integer> output = new Tuple2<>(sale, sum);
                    state.update(sum);
                    return output;
                };
        final JavaMapWithStateDStream<Sale, Integer, Integer, Tuple2<Sale, Integer>> orders = aggregatedSales.mapWithState(StateSpec.function(mappingFunc)
                                                                                                                                    .initialState(initialRdd)
                                                                                                                                    .timeout(Minutes
                                                                                                                                            .apply(10)));


        final JavaPairDStream<Sale, Integer> stateStream = orders.stateSnapshots();
        aggregatedSales.foreachRDD(rdd -> {
            rdd.map(tuple -> tuple).sortBy(tuple -> tuple._2, false, 1)
               .take(1)
               .forEach(System.out::println);
        });

        streamContext.start();
        streamContext.awaitTermination();
    }
}
