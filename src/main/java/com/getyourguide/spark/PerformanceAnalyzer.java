package com.getyourguide.spark;

import com.getyourguide.model.AdDetails;
import com.getyourguide.model.CompanyPerformance;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
public class PerformanceAnalyzer {

    private static final String CSV_SEPARATOR = ",";

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


        System.out.println(rawPerformance.take(10));
    }

    private static CompanyPerformance calculatePerformance(AdDetails ad){
        double consumption = ad.getImpressions() * ad.getCtr() * ad.getCost();
        double performance = ad.getRevenue() / consumption;
        return new CompanyPerformance(ad.getCompany(), performance);
    }

}
