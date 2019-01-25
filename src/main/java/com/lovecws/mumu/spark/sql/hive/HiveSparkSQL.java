package com.lovecws.mumu.spark.sql.hive;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 使用hive表
 * @date 2018-02-06 15:50
 */
public class HiveSparkSQL implements Serializable {

    private HiveContext hiveContext = new MumuSparkConfiguration().hiveContext();

    public void hive() {
        hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        hiveContext.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

        hiveContext.sql("SELECT * FROM src").show();
    }

    public void apk() {
        //Dataset<Row> rowDataset = hiveContext.sql("select decodebase64(u.url) url,sum(u.visitscount) visitscount,d.idcname from isdms.t_ods_idc_activeurl u left join isdms.t_dwd_idc_data d on( u.idcid=d.idcid and d.idcname is not null) where u.ds=" + ds + " and u.hour=" + hour + " group by u.url,d.idcname) activeurl where url like '%.apk%'");
        Dataset<Row> rowDataset = hiveContext.sql("show databases");
        //Dataset<Row> rowDataset = hiveContext.sql("select 'bG92ZWN3cw==',decodebase64('bG92ZWN3cw==') from isdms.t_ods_idc_activeurl limit 10");
        rowDataset.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    System.out.println(row.mkString());
                }
            }
        });
    }
}
