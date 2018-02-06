package com.lovecws.mumu.spark.rdd.wordcount;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: wordCount 测试
 * @date 2018-02-05 15:30
 */
public class SparkWordCountTest {

    @Test
    public void wordCount() {
        new SparkWordCount().wordcount(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/file");
    }

    @Test
    public void userDir() {
        String userDir = System.getProperty("user.dir");
        System.out.println(userDir);
    }
}
