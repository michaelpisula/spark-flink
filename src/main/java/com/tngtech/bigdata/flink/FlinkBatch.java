package com.tngtech.bigdata.flink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkBatch {


    public static void main(final String[] args) throws Exception {


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final String salesPath = "sales.csv";

        env.readCsvFile(salesPath)
           .ignoreInvalidLines()
           .types(Integer.class, Integer.class, String.class)
           .groupBy(0, 2)
           .sum(1)
           .sortPartition(1, Order.DESCENDING)
           .print();


    }

}
