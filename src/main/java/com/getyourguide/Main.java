package com.getyourguide;

import com.getyourguide.spark.PerformanceAnalyzer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
public class Main {

    private static final String DEFAULT_INPUT_FILE = "take_home_test_data.csv";

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
            .builder()
            .appName("Naive company performance analyzer")
            .master("local[*]")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()) {

            String fileName = args.length > 0 ? args[0] : Main.class.getClassLoader().getResource(DEFAULT_INPUT_FILE).getFile();

            new PerformanceAnalyzer(fileName).analyze(spark);
        }
    }

}
