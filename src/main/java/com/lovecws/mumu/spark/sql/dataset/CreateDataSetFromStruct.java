package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过json方式创建dataset
 * @date 2018-02-06 12:34
 */
public class CreateDataSetFromStruct {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

    /**
     * 通过结构体创建
     */
    public void createDataSetFromStruct() {
        StructField nameStructField = new StructField("name", DataTypes.StringType, true, Metadata.empty());
        StructField ageStructField = new StructField("age", DataTypes.IntegerType, true, Metadata.empty());
        StructField sexStructField = new StructField("sex", DataTypes.StringType, true, Metadata.empty());
        StructField birthdayStructField = new StructField("birthday", DataTypes.DateType, true, Metadata.empty());
        StructType structType = new StructType(new StructField[]{nameStructField, ageStructField, sexStructField, birthdayStructField});

        List<Row> rows = new ArrayList<Row>();
        try {
            for (int i = 0; i < 10000; i++) {
                rows.add(new GenericRow(new Object[]{"ganliang" + i, 27 + i, "男" + i, new Date(DateUtils.parseDate("1990-07-07", "yyyy-MM-dd").getTime())}));
                rows.add(new GenericRow(new Object[]{"cws" + i, 25 + i, "女" + i, new Date(DateUtils.parseDate("1992-02-30", "yyyy-MM-dd").getTime())}));
                rows.add(new GenericRow(new Object[]{"youzi" + i, i, "男" + i, new Date(DateUtils.parseDate("2017-02-20 10:43", "yyyy-MM-dd HH:mm").getTime())}));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
        dataset.printSchema();
        dataset.show();

        try {
            dataset.createTempView("person");

            sqlContext.sql("select *from person").show();
            sqlContext.sql("select max(age) from person").show();

            dataset.select(functions.col("name"), functions.col("age")).show();
            dataset.select(functions.max("age")).show();

            dataset.write().parquet(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/sparksql/data/person.parquet");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
