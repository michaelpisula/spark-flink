package com.tngtech.bigdata.spark;

import com.tngtech.bigdata.util.Sale;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkBatch {

    public static void main(final String[] args) {

        // Erzeugen eines lokalen Spark Clusters
        final SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local[*]");
        final JavaSparkContext sparkContext = new JavaSparkContext(conf);

        final String salesPath = "sales.csv";

        final JavaRDD<String> salesFile = sparkContext.textFile(salesPath);
        final JavaPairRDD<Sale, Integer> salesWithAmount = salesFile
                .map(line -> line.split(",")) // Splitten des CSV in einzelne Felder
                .filter(array -> array.length == 3) // Rausfiltern ungültiger Zeilen
                .mapToPair((String[] parts) -> { // Umwandeln der Werte in einen POJO
                    final int id = Integer.parseInt(parts[0]);
                    final int amount = Integer.parseInt(parts[1]);
                    final String date = parts[2];
                    return new Tuple2<>(new Sale(id, date), amount);
                });

        final JavaPairRDD<Sale, Integer> saleWithSum = salesWithAmount.reduceByKey((x, y) -> x + y);

        saleWithSum.rdd().toJavaRDD()
                   .sortBy(Tuple2::_2, false, 1)
                   .foreach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));
    }

}
