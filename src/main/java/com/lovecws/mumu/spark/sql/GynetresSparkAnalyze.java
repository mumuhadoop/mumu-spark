package com.lovecws.mumu.spark.sql;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * @program: mumu-spark
 * @description: spark分析资产探测数据
 * @author: 甘亮
 * @create: 2019-11-22 10:02
 **/
public class GynetresSparkAnalyze implements Serializable {

    public void analyze() {
        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
        Dataset<Row> rowDataset = sqlContext.read().json("E:\\data\\gynetres\\result.json");

        try {
            rowDataset.createTempView("gynetres");
            rowDataset.sqlContext().sql("select service_name,count(1) as counter from gynetres where port=8333  group by service_name order by counter desc").show(100);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    public void deviceSrcondaryNamecnAnalyze() {
        String filePath = "G:\\ACTPython\\trunk\\test\\cache\\gynetres.json";
        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
        Dataset<Row> rowDataset = sqlContext.read().json(filePath);

        try {
            rowDataset.printSchema();
            rowDataset.createTempView("gynetres");
            rowDataset.sqlContext().sql("select device.secondary.namecn,count(1) as counter from gynetres group by device.secondary.namecn order by counter desc").show(100);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
