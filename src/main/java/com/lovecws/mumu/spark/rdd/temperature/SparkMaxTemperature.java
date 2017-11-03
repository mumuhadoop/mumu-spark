package com.lovecws.mumu.spark.rdd.temperature;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 使用spark方式计算最大气温
 * @date 2017-10-30 11:27
 */
public class SparkMaxTemperature implements Serializable {

    public void maxTemperature(List<String> temperatureList) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<String> javaRDD = sparkContext.parallelize(temperatureList);
        JavaRDD mapRDD = javaRDD.map(new Function<String, Object>() {
            @Override
            public Object call(final String o) throws Exception {
                double temperature = Double.parseDouble(o);
                System.out.println(temperature);
                if (temperature < 50 && temperature > 30) {
                    return String.valueOf(temperature);
                }
                return Integer.MIN_VALUE;
            }
        });
        System.out.println("mapRDD:" + StringUtils.join(mapRDD.collect()));

        Object maxTemperature = mapRDD.reduce(new Function2() {
            @Override
            public Object call(final Object o, final Object o2) throws Exception {
                return Double.parseDouble(o.toString()) > Double.parseDouble(o2.toString()) ? o : o2;
            }
        });
        System.out.println("maxTemperature:" + maxTemperature);

        sparkContext.close();
    }
}
