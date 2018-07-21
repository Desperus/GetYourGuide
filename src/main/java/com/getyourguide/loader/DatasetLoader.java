package com.getyourguide.loader;

import com.getyourguide.model.AdDetails;
import com.getyourguide.model.CompanyPerformance;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
@RequiredArgsConstructor
public class DatasetLoader {

    private final String fileName;
    private final SparkSession spark;

    public Dataset<CompanyPerformance> load() {
        return spark.read()
            .option("header", "true")
            .csv(fileName)
            .map((MapFunction<Row, AdDetails>)AdDetails::of, Encoders.bean(AdDetails.class))
            .map((MapFunction<AdDetails, CompanyPerformance>)DatasetLoader::calculatePerformance, Encoders.bean(CompanyPerformance.class))
            .cache();
    }

    private static CompanyPerformance calculatePerformance(AdDetails ad) {
        double consumption = ad.getImpressions() * ad.getCtr() * ad.getCost();
        double performance = ad.getRevenue() / consumption;
        return new CompanyPerformance(ad.getCompany(), performance, ad.getPosition());
    }

}
