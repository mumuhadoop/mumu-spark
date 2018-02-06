package com.lovecws.mumu.spark.sql.dataset;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过反射来生成dataset数据集
 * @date 2018-02-06 12:46
 */
public class CreateDataSetFromReflectTest {

    @Test
    public void createDataSetFromReflect() {
        new CreateDataSetFromReflect().createDataSetFromReflect();
    }

    @Test
    public void createDataSetFromType() {
        new CreateDataSetFromReflect().createDataSetFromType();
    }
}
