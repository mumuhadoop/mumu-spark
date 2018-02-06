package com.lovecws.mumu.spark.sql;

import com.lovecws.mumu.spark.sql.aggregate.MyAverage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: schemardds
 * @date 2017-10-31 16:13
 */
public class BaseSparkSQL {

    public Dataset<Row> rowDataSet;

    public BaseSparkSQL(final Dataset<Row> rowDataSet) {
        this.rowDataSet = rowDataSet;
    }

    public void print() {
        rowDataSet.printSchema();
        rowDataSet.show();
    }

    public void count() {
        long count = rowDataSet.count();
        System.out.println(count);
    }

    public void select(String col, String... cols) {
        rowDataSet.select(col, cols).show();
    }

    public void groupBy(String col, String... cols) {
        rowDataSet.groupBy(col, cols).count().show();
    }

    public void columns() {
        String[] columns = rowDataSet.columns();
        for (String column : columns) {
            System.out.println(column);
        }
    }

    public void sql() {
        rowDataSet.createOrReplaceTempView("bill");
        rowDataSet.sqlContext().sql("select *from bill").show();
    }

    public void aggregate() {
        rowDataSet.sqlContext().udf().register("myAverage", new MyAverage());
        rowDataSet.createOrReplaceTempView("bill");
        rowDataSet.sqlContext().sql("select myAverage(bill_money) from bill").show();
    }
}
