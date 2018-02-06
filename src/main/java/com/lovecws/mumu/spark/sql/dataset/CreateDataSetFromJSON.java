package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过json方式创建dataset
 * @date 2018-02-06 12:34
 */
public class CreateDataSetFromJSON {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

    /**
     * 从json字符串中获取schema 来生成dataset
     *
     * @param jsonStr
     */
    public void createDataSetFromJsonString(String jsonStr) {
        List<String> jsonData = Arrays.asList(jsonStr);
        Dataset<String> dataset = sqlContext.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> jsonDataSet = sqlContext.read().json(dataset);

        jsonDataSet.printSchema();
        jsonDataSet.show();
    }

    /**
     * 从json文件中获取schemaRDD
     *
     * @param jsonPath
     */
    public void createDatasetFromJsonFile(String jsonPath) {
        Dataset<Row> jsonDataSet = sqlContext.read().json(jsonPath);
        jsonDataSet.printSchema();
        jsonDataSet.show();
    }
}
