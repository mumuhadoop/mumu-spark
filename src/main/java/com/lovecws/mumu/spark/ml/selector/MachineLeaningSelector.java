package com.lovecws.mumu.spark.ml.selector;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: tf-idf数据抽取
 * @date 2018-02-07 16:48
 */
public class MachineLeaningSelector {

    private SQLContext spark = new MumuSparkConfiguration().sqlContext();

    public void selectors() {
        Attribute[] attrs = {
                NumericAttribute.defaultAttr().withName("f1"),
                NumericAttribute.defaultAttr().withName("f2"),
                NumericAttribute.defaultAttr().withName("f3")
        };
        AttributeGroup group = new AttributeGroup("userFeatures", attrs);

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(3, new int[]{0, 1}, new double[]{-2.0, 2.3})),
                RowFactory.create(Vectors.dense(-2.0, 2.3, 0.0))
        );

        Dataset<Row> dataset =
                spark.createDataFrame(data, (new StructType()).add(group.toStructField()));

        VectorSlicer vectorSlicer = new VectorSlicer()
                .setInputCol("userFeatures").setOutputCol("features");

        vectorSlicer.setIndices(new int[]{1}).setNames(new String[]{"f3"});

        Dataset<Row> output = vectorSlicer.transform(dataset);
        output.show(false);
    }

    public void rformula() {
        StructType schema = createStructType(new StructField[]{
                createStructField("id", DataTypes.IntegerType, false),
                createStructField("country", DataTypes.StringType, false),
                createStructField("hour", DataTypes.IntegerType, false),
                createStructField("clicked", DataTypes.DoubleType, false)
        });

        List<Row> data = Arrays.asList(
                RowFactory.create(7, "US", 18, 1.0),
                RowFactory.create(8, "CA", 12, 0.0),
                RowFactory.create(9, "NZ", 15, 0.0)
        );

        Dataset<Row> dataset = spark.createDataFrame(data, schema);
        RFormula formula = new RFormula()
                .setFormula("clicked ~ country + hour")
                .setFeaturesCol("features")
                .setLabelCol("label");
        Dataset<Row> output = formula.fit(dataset).transform(dataset);
        output.select("features", "label").show();
    }

    public void chisqselector() {
        List<Row> data = Arrays.asList(
                RowFactory.create(7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
                RowFactory.create(8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
                RowFactory.create(9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
                new StructField("clicked", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        ChiSqSelector selector = new ChiSqSelector()
                .setNumTopFeatures(1)
                .setFeaturesCol("features")
                .setLabelCol("clicked")
                .setOutputCol("selectedFeatures");

        Dataset<Row> result = selector.fit(df).transform(df);

        System.out.println("ChiSqSelector output with top " + selector.getNumTopFeatures()
                + " features selected");
        result.show();
    }

    public void lsh() {
        List<Row> dataA = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 1.0)),
                RowFactory.create(1, Vectors.dense(1.0, -1.0)),
                RowFactory.create(2, Vectors.dense(-1.0, -1.0)),
                RowFactory.create(3, Vectors.dense(-1.0, 1.0))
        );

        List<Row> dataB = Arrays.asList(
                RowFactory.create(4, Vectors.dense(1.0, 0.0)),
                RowFactory.create(5, Vectors.dense(-1.0, 0.0)),
                RowFactory.create(6, Vectors.dense(0.0, 1.0)),
                RowFactory.create(7, Vectors.dense(0.0, -1.0))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dfA = spark.createDataFrame(dataA, schema);
        Dataset<Row> dfB = spark.createDataFrame(dataB, schema);

        Vector key = Vectors.dense(1.0, 0.0);

        BucketedRandomProjectionLSH mh = new BucketedRandomProjectionLSH()
                .setBucketLength(2.0)
                .setNumHashTables(3)
                .setInputCol("features")
                .setOutputCol("hashes");

        BucketedRandomProjectionLSHModel model = mh.fit(dfA);

        System.out.println("The hashed dataset where hashed values are stored in the column 'hashes':");
        model.transform(dfA).show();

        System.out.println("Approximately joining dfA and dfB on distance smaller than 1.5:");
        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
                .select(col("datasetA.id").alias("idA"),
                        col("datasetB.id").alias("idB"),
                        col("EuclideanDistance")).show();

        System.out.println("Approximately searching dfA for 2 nearest neighbors of the key:");
        model.approxNearestNeighbors(dfA, key, 2).show();
    }

    public void minhash() {
        List<Row> dataA = Arrays.asList(
                RowFactory.create(0, Vectors.sparse(6, new int[]{0, 1, 2}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(1, Vectors.sparse(6, new int[]{2, 3, 4}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(2, Vectors.sparse(6, new int[]{0, 2, 4}, new double[]{1.0, 1.0, 1.0}))
        );

        List<Row> dataB = Arrays.asList(
                RowFactory.create(0, Vectors.sparse(6, new int[]{1, 3, 5}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(1, Vectors.sparse(6, new int[]{2, 3, 5}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(2, Vectors.sparse(6, new int[]{1, 2, 4}, new double[]{1.0, 1.0, 1.0}))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dfA = spark.createDataFrame(dataA, schema);
        Dataset<Row> dfB = spark.createDataFrame(dataB, schema);

        int[] indices = {1, 3};
        double[] values = {1.0, 1.0};
        Vector key = Vectors.sparse(6, indices, values);

        MinHashLSH mh = new MinHashLSH()
                .setNumHashTables(5)
                .setInputCol("features")
                .setOutputCol("hashes");

        MinHashLSHModel model = mh.fit(dfA);

        System.out.println("The hashed dataset where hashed values are stored in the column 'hashes':");
        model.transform(dfA).show(false);

        System.out.println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:");
        model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
                .select(col("datasetA.id").alias("idA"),
                        col("datasetB.id").alias("idB"),
                        col("JaccardDistance")).show();

        System.out.println("Approximately searching dfA for 2 nearest neighbors of the key:");
        model.approxNearestNeighbors(dfA, key, 2).show(false);
    }
}
