package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过parquet来创建dataset
 * @date 2018-02-06 12:43
 */
public class CreateDataSetFromParquetTest {

    @Test
    public void createDataSetFromParquet() {
        String hadoopAddress = new MumuSparkConfiguration().hadoopAddress();
        new CreateDataSetFromParquet().createDataSetFromParquet(hadoopAddress + "/mumu/spark/sparksql/data/bill.parquet");
    }
}
