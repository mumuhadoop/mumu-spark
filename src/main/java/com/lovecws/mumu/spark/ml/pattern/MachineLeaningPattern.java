package com.lovecws.mumu.spark.ml.pattern;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 正则表达式
 * @date 2018-02-08 10:26
 */
public class MachineLeaningPattern {

    private SQLContext spark = new MumuSparkConfiguration().sqlContext();

    public void FPGrowth() {
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("1 2 5".split(" "))),
                RowFactory.create(Arrays.asList("1 2 3 5".split(" "))),
                RowFactory.create(Arrays.asList("1 2".split(" ")))
        );
        StructType schema = new StructType(new StructField[]{new StructField(
                "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> itemsDF = spark.createDataFrame(data, schema);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(0.5)
                .setMinConfidence(0.6)
                .fit(itemsDF);

        model.freqItemsets().show();

        model.associationRules().show();

        model.transform(itemsDF).show();
    }
}
