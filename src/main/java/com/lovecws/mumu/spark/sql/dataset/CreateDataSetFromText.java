package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过json方式创建dataset
 * @date 2018-02-06 12:34
 */
public class CreateDataSetFromText {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

    /**
     * 通过实体对象反射
     */
    public void createDataSetFromText(String textFile) {
        Dataset<String> dataset = sqlContext.read().textFile(textFile);
        JavaRDD<String> javaRDD = dataset.javaRDD();
        JavaRDD<Object> objectJavaRDD = javaRDD.map(new Function<String, Object>() {
            @Override
            public Object call(final String line) throws Exception {
                String[] split = line.split(",");
                return split;
            }
        });
        Dataset<Row> dataFrame = sqlContext.createDataFrame(objectJavaRDD, String[].class);
        dataFrame.printSchema();
        dataFrame.show();
    }
}
