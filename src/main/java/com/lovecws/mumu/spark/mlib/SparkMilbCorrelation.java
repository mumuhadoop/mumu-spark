package com.lovecws.mumu.spark.mlib;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 计算数据相关性 pearson,spearman
 *                 http://blog.csdn.net/xubo245/article/details/51485020
 * @date 2017-11-02 14:52
 */
public class SparkMilbCorrelation {
    public static void main(String[] args) {

        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, -2.0})),
                RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
                RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{9.0, 1.0}))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = sqlContext.createDataFrame(data, schema);

        df.createOrReplaceTempView("feature");
        Dataset<Row> rowDataset = sqlContext.sql("select *from feature");
        rowDataset.show();

        Row r1 = Correlation.corr(df, "features","pearson").head();
        System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

        Row r2 = Correlation.corr(df, "features", "spearman").head();
        System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
    }
}
