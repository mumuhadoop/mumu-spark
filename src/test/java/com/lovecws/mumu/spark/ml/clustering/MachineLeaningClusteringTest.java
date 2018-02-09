package com.lovecws.mumu.spark.ml.clustering;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 聚类测试
 * @date 2018-02-08 15:07
 */
public class MachineLeaningClusteringTest {

    private MachineLeaningClustering clustering = new MachineLeaningClustering();

    @Test
    public void kmeans() {
        clustering.kmeans();
    }

    @Test
    public void lda() {
        clustering.lda();
    }

    @Test
    public void bisectingKMeans() {
        clustering.bisectingKMeans();
    }

    @Test
    public void gaussianMixture() {
        clustering.gaussianMixture();
    }
}
