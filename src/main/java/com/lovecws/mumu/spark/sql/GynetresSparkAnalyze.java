package com.lovecws.mumu.spark.sql;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

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
        //String filePath = "G:\\databack\\gynetres\\20191203\\industry_gynetres_20190807.json";
        String filePath = "G:\\ACTPython\\gynetres\\trunk\\test\\cache\\gynetres.json";
//        String filePath = "G:\\ACTPython\\gynetres\\trunk\\test\\cache\\bugynetres.json";
//        String filePath = "E:\\data\\mumuflink\\atd\\localfile\\gynetres";
        //String filePath = "G:\\\\databack\\\\gynetres\\\\20191203\\industry_gynetres_20191100.json";
//        String filePath = "G:\\databack\\gynetres\\20191204\\industry_gynetres_20191100";

        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
        Dataset<Row> rowDataset = sqlContext.read().json(filePath);
        rowDataset.persist(StorageLevel.MEMORY_AND_DISK_2());

        try {
            rowDataset.printSchema();
            rowDataset.createTempView("gynetres");
//            rowDataset.sqlContext().sql("select device.primary.name as devicePrimaryName,device.primary.namecn as devicePrimaryNamecn,device.secondary.namecn as deviceSecondaryNamecn,service as service,count(1) as counter from gynetres group by device.primary.name,device.primary.namecn,device.secondary.name,device.secondary.namecn,service order by counter desc").show(100);
//            rowDataset.sqlContext().sql("select device.primary.namecn as devicePrimaryName,count(1) as counter from gynetres group by device.primary.namecn order by counter desc").show(100);
//            rowDataset.sqlContext().sql("select service as service,count(1) as counter from gynetres where device.primary.namecn='未知' group by service order by counter desc").show(100);
//            rowDataset.sqlContext().sql("select device.secondary.namecn,service,count(1) as counter from gynetres group by device.secondary.namecn,service order by counter desc").show(100);
//            rowDataset.sqlContext().sql("select device.secondary.namecn,service,count(1) as counter from gynetres where device.primary.namecn='物联网设备' group by device.secondary.namecn,service order by counter desc").show(100);
//            rowDataset.sqlContext().sql("select vendor,vendor_source,count(1) as counter from gynetres  group by vendor,vendor_source order by counter desc").show(100);
//            rowDataset.sqlContext().sql("select distinct vendor from gynetres").coalesce(1).show(100);
//            rowDataset.sqlContext().sql("select device.primary.name as devicePrimaryName,device.primary.namecn as devicePrimaryNamecn,device.secondary.name as deviceSecondaryName,device.secondary.namecn as deviceSecondaryNamecn from gynetres where service='http'").coalesce(1).show(100);
            rowDataset.sqlContext().sql("select to_date(create_time,'yyyy-MM-dd'),count(1) counter from gynetres group by to_date(create_time,'yyyy-MM-dd')").coalesce(1).show(100);
//            rowDataset.sqlContext().sql("select count(1) counter from gynetres where create_time>update_time").coalesce(1).show(100);

        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
