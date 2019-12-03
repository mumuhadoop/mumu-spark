package com.lovecws.mumu.spark.sql;

import org.junit.Test;

/**
 * @program: mumu-spark
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-11-22 10:09
 **/
public class GynetresSparkAnalyzeTest {

    @Test
    public void analyze() {
        new GynetresSparkAnalyze().analyze();
    }
    @Test
    public void deviceSrcondaryNamecnAnalyze() {
        new GynetresSparkAnalyze().deviceSrcondaryNamecnAnalyze();
    }
}
