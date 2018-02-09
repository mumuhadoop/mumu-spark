package com.lovecws.mumu.spark.ml.statistics;

import com.lovecws.mumu.spark.ml.statistics.MachineLeaningCorrelation;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 机器学习语言
 * @date 2018-02-07 15:58
 */
public class MachineLeaningCorrelationTest {

    private MachineLeaningCorrelation correlation = new MachineLeaningCorrelation();

    @Test
    public void pearson() {
        correlation.pearson();
    }

    @Test
    public void spearman() {
        correlation.spearman();
    }


    @Test
    public void statistics() {
        correlation.statistics();
    }
}
