package com.lovecws.mumu.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import java.util.Map;

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
}
