package com.getyourguide.spark;

import com.getyourguide.model.AdDetails;
import com.getyourguide.model.CompanyPerformance;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.sort_array;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
public class PerformanceAnalyzer {

    private static final String OUTPUT_DIR = "out_analysis";

    private String fileName;

    public PerformanceAnalyzer(String fileName) {
        this.fileName = fileName;
    }

    public void analyze(SparkSession spark) {
        Dataset<CompanyPerformance> rawPerformance = spark.read()
            .option("header", "true")
            .csv(fileName)
            .map((MapFunction<Row, AdDetails>)AdDetails::of, Encoders.bean(AdDetails.class))
            .map((MapFunction<AdDetails, CompanyPerformance>)PerformanceAnalyzer::calculatePerformance, Encoders.bean(CompanyPerformance.class))
            .cache();

        RelationalGroupedDataset byCompany = rawPerformance.groupBy("company");

        Dataset<Row> median = byCompany
            .agg(sort_array(collect_list("performance")))
            .map((MapFunction<Row, Tuple2<String, Double>>)row -> {
                List<Double> perf = row.getList(1);
                return new Tuple2<>(row.getString(0), perf.get(perf.size() / 2));
            }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()))
            .toDF("Company", "Median");
        saveToCsv(median, "median");
        saveToCsv(byCompany.count(), "counts");
        saveToCsv(rawPerformance, "raw_performance");
    }

    private static CompanyPerformance calculatePerformance(AdDetails ad) {
        double consumption = ad.getImpressions() * ad.getCtr() * ad.getCost();
        double performance = ad.getRevenue() / consumption;
        return new CompanyPerformance(ad.getCompany(), performance);
    }

    private static void saveToCsv(Dataset dataset, String name) {
        dataset
            .coalesce(1) // as I know that right now size is small
            .write()
            .mode(SaveMode.Overwrite)
            .csv(Paths.get(OUTPUT_DIR).resolve(name).toAbsolutePath().toString());
    }

}
