package com.lovecws.mumu.spark.ml.filtering;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 协同过滤测试
 * @date 2018-02-08 15:53
 */
public class MachineLeaningFilteringTest {

    private MachineLeaningFiltering filtering = new MachineLeaningFiltering();

    @Test
    public void itemRecommend() {
        filtering.itemRecommend();
    }

    @Test
    public void recommend() {
        filtering.recommend();
    }

    @Test
    public void evaluate() {
        filtering.evaluate();
    }
}
