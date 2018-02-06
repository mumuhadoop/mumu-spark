package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过json方式创建dataset
 * @date 2018-02-06 12:34
 */
public class CreateDataSetFromParquet {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

    /**
     * 从parquet文件中获取schemaRDD
     */
    public void createDataSetFromParquet(String parquetPath) {
        Dataset<Row> dataset = sqlContext.read().parquet(parquetPath);

        dataset.printSchema();
        dataset.show();
    }
}
