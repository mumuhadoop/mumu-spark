package com.lovecws.mumu.spark.ml.selection;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import com.lovecws.mumu.spark.ml.pipeline.JavaDocument;
import com.lovecws.mumu.spark.ml.pipeline.JavaLabeledDocument;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 模型选择
 * @date 2018-02-08 10:28
 */
public class MachineLeaningSelection {

    private SQLContext spark = new MumuSparkConfiguration().sqlContext();

    public void crossValidation() {
        Dataset<Row> training = spark.createDataFrame(Arrays.asList(
                new JavaLabeledDocument(0L, "a b c d e spark", 1.0),
                new JavaLabeledDocument(1L, "b d", 0.0),
                new JavaLabeledDocument(2L, "spark f g h", 1.0),
                new JavaLabeledDocument(3L, "hadoop mapreduce", 0.0),
                new JavaLabeledDocument(4L, "b spark who", 1.0),
                new JavaLabeledDocument(5L, "g d a y", 0.0),
                new JavaLabeledDocument(6L, "spark fly", 1.0),
                new JavaLabeledDocument(7L, "was mapreduce", 0.0),
                new JavaLabeledDocument(8L, "e spark program", 1.0),
                new JavaLabeledDocument(9L, "a e c l", 0.0),
                new JavaLabeledDocument(10L, "spark compile", 1.0),
                new JavaLabeledDocument(11L, "hadoop software", 0.0)
        ), JavaLabeledDocument.class);

        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.01);
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(hashingTF.numFeatures(), new int[]{10, 100, 1000})
                .addGrid(lr.regParam(), new double[]{0.1, 0.01})
                .build();

        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new BinaryClassificationEvaluator())
                .setEstimatorParamMaps(paramGrid).setNumFolds(2);  // Use 3+ in practice

        CrossValidatorModel cvModel = cv.fit(training);

        Dataset<Row> test = spark.createDataFrame(Arrays.asList(
                new JavaDocument(4L, "spark i j k"),
                new JavaDocument(5L, "l m n"),
                new JavaDocument(6L, "mapreduce spark"),
                new JavaDocument(7L, "apache hadoop")
        ), JavaDocument.class);

        Dataset<Row> predictions = cvModel.transform(test);
        for (Row r : predictions.select("id", "text", "probability", "prediction").collectAsList()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }
    }

    public void train() {
        Dataset<Row> data = spark.read().format("libsvm")
                .load("data/mllib/sample_linear_regression_data.txt");

        Dataset<Row>[] splits = data.randomSplit(new double[]{0.9, 0.1}, 12345);
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        LinearRegression lr = new LinearRegression();

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(lr.regParam(), new double[]{0.1, 0.01})
                .addGrid(lr.fitIntercept())
                .addGrid(lr.elasticNetParam(), new double[]{0.0, 0.5, 1.0})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(lr)
                .setEvaluator(new RegressionEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setTrainRatio(0.8);

        TrainValidationSplitModel model = trainValidationSplit.fit(training);

        model.transform(test)
                .select("features", "label", "prediction")
                .show();
    }
}
