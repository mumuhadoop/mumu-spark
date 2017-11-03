package com.lovecws.mumu.spark.rdd.temperature;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 测试最大温度
 * @date 2017-10-30 13:56
 */
public class maxTemperatureTest {

    public static List<String> temperatureList = new ArrayList<String>();
    private SparkMaxTemperature sparkMaxTemperature = new SparkMaxTemperature();

    static {
        temperatureList.add("37.14");
        temperatureList.add("37.11");
        temperatureList.add("37.18");
        temperatureList.add("37.90");
        temperatureList.add("36.00");
        temperatureList.add("38.50");
        temperatureList.add("39.01");
    }

    @Test
    public void maxTemperature() {
        sparkMaxTemperature.maxTemperature(temperatureList);
    }
}
