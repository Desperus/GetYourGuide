package com.getyourguide.spark;

import com.getyourguide.loader.DatasetLoader;
import com.getyourguide.model.CompanyPerformance;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.sort_array;

/**
 * As tech task states single metrics for each company should be calculated. This class takes all data from csv and
 * calculates single number of ad performance for each company. To calculate that performance it relies on the fact
 * the highest position has more balance between revenue and costs so it promotes ads companies with higher position
 * by multiplying performance by a position number. After that it takes median value for overall vision of performance
 * across all ads disregarding their number.
 *
 * Author: Aleksander
 * Since: 21.07.2018.
 */
public class PerformanceMetricCalculator {

    private static final String COMPANY_KEY = "Company";
    private static final String METRIC_KEY = "Metric";

    public Map<String, Double> calculate(DatasetLoader loader) {
        Row[] result = (Row[]) loader.load()
            .map((MapFunction<CompanyPerformance, Tuple2<String, Double>>)row ->
                    new Tuple2<>(row.getCompany(), row.getPerformance() * row.getPosition()),
                Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()))
            .toDF(COMPANY_KEY, METRIC_KEY)
            .groupBy(COMPANY_KEY)
            .agg(sort_array(collect_list(METRIC_KEY)))
            .map((MapFunction<Row, Tuple2<String, Double>>)row -> {
                List<Double> perf = row.getList(1);
                return new Tuple2<>(row.getString(0), perf.get(perf.size() / 2));
            }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()))
            .toDF(COMPANY_KEY, METRIC_KEY)
            .sort(desc(METRIC_KEY))
            .collect();

        return Arrays.stream(result).collect(LinkedHashMap::new, (map, row) -> map.put(row.getString(0), row.getDouble(1)), LinkedHashMap::putAll);
    }
}
