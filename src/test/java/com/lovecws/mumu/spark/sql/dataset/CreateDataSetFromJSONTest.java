package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: json数据集测试
 * @date 2018-02-06 12:35
 */
public class CreateDataSetFromJSONTest {

    @Test
    public void createDataSetFromJsonString() {
        new CreateDataSetFromJSON().createDataSetFromJsonString("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
    }

    @Test
    public void createDatasetFromJsonFile() {
        new CreateDataSetFromJSON().createDatasetFromJsonFile(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/sparksql/data/bill.json");
    }
}
