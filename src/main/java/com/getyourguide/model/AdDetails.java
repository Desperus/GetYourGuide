package com.getyourguide.model;

import com.getyourguide.exception.ValidationException;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Row;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdDetails implements Serializable {

    private static final int FIELDS_NUMBER = 7;

    private String keyword;
    private int impressions;
    private double ctr;
    private double cost;
    private int position;
    private String company;
    private double revenue;

    public static AdDetails of(Row row) {
        if (row.size() != FIELDS_NUMBER) {
            throw new ValidationException(String.format("Expected to have %s fields but got %s", FIELDS_NUMBER, row.size()));
        }
        try {
            int impressions = Integer.parseInt(row.getString(1));
            double ctr = Double.parseDouble(row.getString(2));
            double cost = Double.parseDouble(row.getString(3));
            int position = Integer.parseInt(row.getString(4));
            double revenue = Double.parseDouble(row.getString(6));
            return new AdDetails(row.getString(0), impressions, ctr, cost, position, row.getString(5), revenue);
        } catch (Exception e) {
            throw new ValidationException("Could not parse columns into entity: " + row, e);
        }
    }

}
