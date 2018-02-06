package com.lovecws.mumu.spark.sql.dataset;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过数据库来构建dataset数据集
 * @date 2018-02-06 12:49
 */
public class CreateDataSetFromDatabaseTest {

    @Test
    public void createDataSetFromDatabase() {
        new CreateDataSetFromDatabase().createDataSetFromDatabase("jdbc:mysql://192.168.11.25:3307/elebill_test","ec_bill","root","123");
    }
}
