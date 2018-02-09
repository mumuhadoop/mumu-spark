package com.lovecws.mumu.spark.ml.filtering;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 协作过滤 推荐
 * @date 2018-02-08 10:23
 */
public class MachineLeaningFiltering {

    private SQLContext spark = new MumuSparkConfiguration().sqlContext();

    public void itemRecommend() {
        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile("data/mllib/als/itemmodels.csv").javaRDD()
                .map(new Function<String, Rating>() {
                    @Override
                    public Rating call(final String line) throws Exception {
                        return Rating.parseRatingCsv(line);
                    }
                });
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);

        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(ratings);
        //给用户推荐的物品
        Dataset<Row> userRecs = model.recommendForAllUsers(10);
        userRecs.show(false);

        //给物品推荐的用户
        Dataset<Row> movieRecs = model.recommendForAllItems(10);
        movieRecs.show(false);
    }

    /**
     * 推荐
     */
    public void recommend() {
        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile("data/mllib/als/sample_movielens_ratings.txt").javaRDD()
                .map(Rating::parseRating);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);

        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(ratings);
        //给用户推荐的物品
        Dataset<Row> userRecs = model.recommendForAllUsers(10);
        userRecs.show(false);

        //给物品推荐的用户
        Dataset<Row> movieRecs = model.recommendForAllItems(10);
        movieRecs.show(false);
    }

    /**
     * 评估推荐
     */
    public void evaluate() {
        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile("data/mllib/als/sample_movielens_ratings.txt").javaRDD()
                .map(Rating::parseRating);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);
    }
}
