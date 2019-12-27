package com.lovecws.mumu.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Executors;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: spark配置文件测试
 * @date 2017-10-30 13:42
 */
public class MumuSparkConfigurationTest {

    private MumuSparkConfiguration sparkConfiguration = new MumuSparkConfiguration();

    @Test
    public void javaSparkContext() {
        JavaSparkContext sparkContext = sparkConfiguration.javaSparkContext();
        System.out.println(sparkContext);
    }

    @Test
    public void javaStreamingContext() {
        JavaStreamingContext streamingContext = sparkConfiguration.javaStreamingContext(10l);
        System.out.println(streamingContext);
    }

    @Test
    public void sqlContext() {
        SQLContext sqlContext = sparkConfiguration.sqlContext();
        System.out.println(sqlContext);
    }

    @Test
    public void hiveContext() {
        SQLContext hiveContext = sparkConfiguration.hiveContext();
        System.out.println(hiveContext);
    }

    @Test
    public void uploadJar() {
        sparkConfiguration.uploadJar();
    }

    public static void main(String[] args) {
        String str="B97F64587D05AC52\t\t\t\t\t\t\t\t\tSMTP\tMail\tSMTP\t61.187.88.126\tFFFFFFFFFFFFFFFF\t27851\t\t220.181.15.113\tFFFFFFFFFFFFFFFF\t25\t246\t210\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tB97F64587D05AC52\tSMTP\t0\t0\t0\t0\t1\t0\t4\t0\t0\tXDR\t1556066695\t1556066860\t101405\t101405000\t\t\t\t\t\t\t\t\t\n";
        String[] split = str.split("\\t+");
        for (String s:split){
            System.out.println(s);
        }
    }
}
