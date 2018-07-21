package com.getyourguide.spark;

import com.getyourguide.loader.DatasetLoader;
import com.getyourguide.model.CompanyPerformance;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;
import scala.Tuple3;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.sort_array;

/**
 * Calculates different metrics for companies along with the performance metric. For performance balance between
 * revenue and costs is chosen where costs are calculated based on ad clicks.
 *
 * Author: Aleksander
 * Since: 21.07.2018.
 */
public class PerformanceAnalyzer {

    private static final String OUTPUT_DIR = "out_analysis";
    private static final String COMPANY_KEY = "Company";
    private static final String MEDIAN_KEY = "Median";
    private static final String POSITION_KEY = "Position";

    public void analyze(DatasetLoader loader) {
        Dataset<CompanyPerformance> rawPerformance = loader.load();

        RelationalGroupedDataset byCompany = rawPerformance.groupBy("company");

        Dataset<Row> median = byCompany
            .agg(sort_array(collect_list("performance")))
            .map((MapFunction<Row, Tuple2<String, Double>>)row -> {
                List<Double> perf = row.getList(1);
                return new Tuple2<>(row.getString(0), perf.get(perf.size() / 2));
            }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()))
            .toDF(COMPANY_KEY, MEDIAN_KEY);

        Dataset<Row> byCompanyAndPosition = rawPerformance.groupBy("company", "position")
            .agg(sort_array(collect_list("performance")))
            .map((MapFunction<Row, Tuple3<String, Integer, Double>>)row -> {
                List<Double> perf = row.getList(2);
                return new Tuple3<>(row.getString(0), row.getInt(1), perf.get(perf.size() / 2));
            }, Encoders.tuple(Encoders.STRING(), Encoders.INT(), Encoders.DOUBLE()))
            .toDF(COMPANY_KEY, POSITION_KEY, MEDIAN_KEY)
            .sort(COMPANY_KEY, POSITION_KEY);

        saveToCsv(median, "median");
        saveToCsv(byCompanyAndPosition, "median_by_position");
        saveToCsv(byCompany.count(), "counts");
        saveToCsv(rawPerformance, "raw_performance");
    }

    private static void saveToCsv(Dataset dataset, String name) {
        dataset
            .coalesce(1) // as I know that right now size is small
            .write()
            .mode(SaveMode.Overwrite)
            .csv(Paths.get(OUTPUT_DIR).resolve(name).toAbsolutePath().toString());
    }

}
