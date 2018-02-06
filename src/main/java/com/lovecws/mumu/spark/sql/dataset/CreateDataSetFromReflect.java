package com.lovecws.mumu.spark.sql.dataset;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import com.lovecws.mumu.spark.sql.entity.PersonEntity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过json方式创建dataset
 * @date 2018-02-06 12:34
 */
public class CreateDataSetFromReflect {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

    /**
     * 通过实体对象反射
     */
    public void createDataSetFromReflect() {
        PersonEntity person = new PersonEntity();
        person.setName("Andy");
        person.setAge(90);
        Encoder<PersonEntity> personEncoder = Encoders.bean(PersonEntity.class);
        Dataset<PersonEntity> dataset = sqlContext.createDataset(Collections.singletonList(person), personEncoder);
        dataset.printSchema();
        dataset.show();
    }

    /**
     * 通过基本数据类型 来生成数据集
     */
    public void createDataSetFromType() {
        Dataset<String> dataset = sqlContext.createDataset(Arrays.asList("cws", "lover", "babymm"), Encoders.STRING());
        dataset.printSchema();
        dataset.show();
    }
}
