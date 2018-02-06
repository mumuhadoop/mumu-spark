package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import java.util.Properties;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过json方式创建dataset
 * @date 2018-02-06 12:34
 */
public class CreateDataSetFromDatabase {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

    public void createDataSetFromDatabase(String url, String table, String user, String password) {
        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);
        Dataset<Row> dataset = sqlContext.read().jdbc(url, table, properties);
        dataset.printSchema();

        dataset.select(functions.sum("bill_id")).show();

        dataset.createOrReplaceTempView("bill");
        sqlContext.sql("select *from bill").show();

        dataset.persist(StorageLevel.MEMORY_ONLY());
        //dataset.write().jdbc(url, table + "_bg", properties);
        dataset.write().json(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/sparksql/data/bill.json");
        dataset.write().parquet(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/sparksql/data/bill.parquet");
        dataset.write().csv(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/sparksql/data/bill.csv");
        dataset.select("bill_user_name").write().text(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/sparksql/data/bill.text");
    }

}
