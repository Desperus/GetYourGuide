package com.getyourguide.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompanyPerformance implements Serializable {

    private String company;
    private double performance;

}
