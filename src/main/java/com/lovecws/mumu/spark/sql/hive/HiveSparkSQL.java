package com.lovecws.mumu.spark.sql.hive;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 使用hive表
 * @date 2018-02-06 15:50
 */
public class HiveSparkSQL {

    private SQLContext sqlContext = new MumuSparkConfiguration().hiveContext();

    public void hive() {
        sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        sqlContext.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

        sqlContext.sql("SELECT * FROM src").show();
    }
}
