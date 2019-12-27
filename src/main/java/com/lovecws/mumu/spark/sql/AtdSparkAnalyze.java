package com.lovecws.mumu.spark.sql;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.*;

/**
 * @program: mumu-spark
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-12-12 20:00
 **/
public class AtdSparkAnalyze {

    public void analyze() {
        String filePath = "E:\\data\\industrydataprocessing\\atd\\storage\\20191212";
        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
        Dataset<Row> rowDataset = sqlContext.read().parquet(filePath);

        rowDataset.show(10);
        try {
            rowDataset.createTempView("atd");
            rowDataset.sqlContext().sql("select count(1) as counter from atd").show(100);
            rowDataset.sqlContext().sql("select count(1) as counter from atd where corp_name is not null or src_corp_name is not null").show(100);
            rowDataset.sqlContext().sql("select count(1) as counter from atd where corp_name is null and src_corp_name is null").show(100);
            rowDataset.sqlContext().sql("select distinct dst_ip from atd where corp_name is null and src_corp_name is null and dst_ip_province='湖南省' union select distinct src_ip from atd where corp_name is null and src_corp_name is null and src_ip_province='湖南省'").coalesce(1).write().mode(SaveMode.Overwrite).csv("E:\\data\\industrydataprocessing\\atd\\corpiphunan");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
