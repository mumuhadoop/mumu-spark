package com.lovecws.mumu.spark.sql;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: schemaRDD创建测试
 * @date 2017-11-01 13:45
 */
public class BaseSparkSQLTest {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
    private BaseSparkSQL sparkSQL = null;

    @Before
    public void before() {
        Dataset<Row> rowDataset = sqlContext.read().json(new MumuSparkConfiguration().hadoopAddress() + "/mumu/spark/sparksql/data/bill.json");
        sparkSQL = new BaseSparkSQL(rowDataset);
    }

    @Test
    public void print() {
        sparkSQL.print();
    }

    @Test
    public void count() {
        sparkSQL.count();
    }

    @Test
    public void select() {
        sparkSQL.select("bill_accept_org");
    }

    @Test
    public void groupBy() {
        sparkSQL.groupBy("bill_classify");
    }

    @Test
    public void columns() {
        sparkSQL.columns();
    }

    @Test
    public void sql() {
        sparkSQL.sql();
    }
    
    @Test
    public void aggregate() {
        sparkSQL.aggregate();
    }
}
