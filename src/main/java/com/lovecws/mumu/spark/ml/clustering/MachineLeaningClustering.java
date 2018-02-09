package com.lovecws.mumu.spark.ml.clustering;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.ml.clustering.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 聚类
 * @date 2018-02-08 10:16
 */
public class MachineLeaningClustering {

    private SQLContext spark = new MumuSparkConfiguration().sqlContext();

    /**
     * kmeans聚类
     */
    public void kmeans() {
        Dataset<Row> dataset = spark.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");
        dataset.show(false);
        KMeans kmeans = new KMeans().
                setK(2).
                setSeed(1L);
        KMeansModel model = kmeans.fit(dataset);

        double WSSSE = model.computeCost(dataset);
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        Vector[] centers = model.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (Vector center : centers) {
            System.out.println(center);
        }
        int predict = model.predict(Vectors.sparse(3, new int[]{0, 1, 2}, new double[]{0.2, 0.2, 0.2}));
        System.out.println("cluster:"+predict);
    }

    public void lda() {
        Dataset<Row> dataset = spark.read().format("libsvm")
                .load("data/mllib/sample_lda_libsvm_data.txt");

        LDA lda = new LDA().setK(10).setMaxIter(10);
        LDAModel model = lda.fit(dataset);

        double ll = model.logLikelihood(dataset);
        double lp = model.logPerplexity(dataset);
        System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
        System.out.println("The upper bound on perplexity: " + lp);

        Dataset<Row> topics = model.describeTopics(3);
        System.out.println("The topics described by their top-weighted terms:");
        topics.show(false);

        Dataset<Row> transformed = model.transform(dataset);
        transformed.show(false);
    }

    public void bisectingKMeans() {
        Dataset<Row> dataset = spark.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");

        BisectingKMeans bkm = new BisectingKMeans().setK(2).setSeed(1);
        BisectingKMeansModel model = bkm.fit(dataset);

        double cost = model.computeCost(dataset);
        System.out.println("Within Set Sum of Squared Errors = " + cost);

        System.out.println("Cluster Centers: ");
        Vector[] centers = model.clusterCenters();
        for (Vector center : centers) {
            System.out.println(center);
        }
    }

    public void gaussianMixture() {
        Dataset<Row> dataset = spark.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");

        GaussianMixture gmm = new GaussianMixture().setK(2);
        GaussianMixtureModel model = gmm.fit(dataset);

        for (int i = 0; i < model.getK(); i++) {
            System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n", i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
        }
    }
}
