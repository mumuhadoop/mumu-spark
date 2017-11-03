package com.lovecws.mumu.spark.sql;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: schemardds
 * @date 2017-10-31 16:13
 */
public class SchemaRDDSparkSQL {

    /**
     * 通过实体对象反射
     */
    public void reflect() {
        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
        Person person = new Person();
        person.setName("Andy");
        person.setAge(90);
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> dataset = sqlContext.createDataset(Collections.singletonList(person), personEncoder);
        dataset.printSchema();
        dataset.show();
    }

    /**
     * 通过结构体创建
     */
    public void struct() {
        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

        StructField nameStructField = new StructField("name", DataTypes.StringType, true, Metadata.empty());
        StructField ageStructField = new StructField("age", DataTypes.IntegerType, true, Metadata.empty());
        StructField sexStructField = new StructField("sex", DataTypes.StringType, true, Metadata.empty());
        StructField birthdayStructField = new StructField("birthday", DataTypes.DateType, true, Metadata.empty());
        StructType structType = new StructType(new StructField[]{nameStructField, ageStructField, sexStructField, birthdayStructField});

        List<Row> rows = new ArrayList<Row>();
        try {
            rows.add(new GenericRow(new Object[]{"ganliang", 27, "男", new Date(DateUtils.parseDate("1990-07-07", "yyyy-MM-dd").getTime())}));
            rows.add(new GenericRow(new Object[]{"cws", 25, "女", new Date(DateUtils.parseDate("1992-02-30", "yyyy-MM-dd").getTime())}));
            rows.add(new GenericRow(new Object[]{"youzi", 0, "男", new Date(DateUtils.parseDate("2017-02-20 10:43", "yyyy-MM-dd HH:mm").getTime())}));
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
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从json文件中获取schemaRDD
     */
    public void json() {
        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = sqlContext.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = sqlContext.read().json(anotherPeopleDataset);
        anotherPeople.show();
    }
}
