package com.getyourguide;

import com.getyourguide.loader.DatasetLoader;
import com.getyourguide.spark.PerformanceAnalyzer;
import com.getyourguide.spark.PerformanceMetricCalculator;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
public class Main {

    private static final String DEFAULT_INPUT_FILE = "take_home_test_data.csv";
    private static final String WIN_UTILS_PATH = "dependencies";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", Paths.get(WIN_UTILS_PATH).toAbsolutePath().toString());

        try (SparkSession spark = SparkSession
            .builder()
            .appName("Naive company performance analyzer")
            .master("local[*]")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()) {

            String fileName = args.length > 0 ? args[0] : Main.class.getClassLoader().getResource(DEFAULT_INPUT_FILE).getFile();

            DatasetLoader loader = new DatasetLoader(fileName, spark);
            // this is only here so you can follow my thoughts on choosing algorithm for metric calculation
            new PerformanceAnalyzer().analyze(loader);
            Map<String, Double> metric = new PerformanceMetricCalculator().calculate(loader);
            System.out.println("Calculated metrics: " + metric);
        }
    }

}
