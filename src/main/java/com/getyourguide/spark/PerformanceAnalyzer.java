package com.getyourguide.spark;

import com.getyourguide.model.AdDetails;
import com.getyourguide.model.CompanyPerformance;
import java.nio.file.Paths;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

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

        saveToCsv(rawPerformance.groupBy("company").count(), "counts");
        saveToCsv(rawPerformance, "raw_performance");
    }

    private static CompanyPerformance calculatePerformance(AdDetails ad){
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
