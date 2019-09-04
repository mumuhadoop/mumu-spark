package com.lovecws.mumu.spark.sql.hive;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * @program: mumu-spark
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-05-05 14:00
 **/
public class DnsLogParse implements Serializable {

    public static void main(String[] args) {

    }

    public void emailLogParse(String filePath) {
        if (filePath == null) return;

        SQLContext sqlContext = new MumuSparkConfiguration().hiveContext();

        Collection<File> files = FileUtils.listFiles(new File(filePath), new String[]{"csv"}, true);

        List<String> filePaths = new ArrayList<>();
        files.forEach(file -> filePaths.add(file.getAbsolutePath()));

        //StructField
        StructField id = new StructField("id", DataTypes.StringType, true, Metadata.empty());
        StructField protocal = new StructField("protocal", DataTypes.StringType, true, Metadata.empty());
        StructField protocal_type = new StructField("protocal_type", DataTypes.StringType, true, Metadata.empty());
        StructField protocal_apply = new StructField("protocal_apply", DataTypes.StringType, true, Metadata.empty());
        StructField protocal_detail = new StructField("protocal_detail", DataTypes.StringType, true, Metadata.empty());
        StructField user_ip = new StructField("user_ip", DataTypes.StringType, true, Metadata.empty());
        StructField user_packet = new StructField("user_packet", DataTypes.StringType, true, Metadata.empty());
        StructField user_port = new StructField("user_port", DataTypes.IntegerType, true, Metadata.empty());
        StructField server_ip = new StructField("server_ip", DataTypes.StringType, true, Metadata.empty());
        StructField server_packet = new StructField("server_packet", DataTypes.StringType, true, Metadata.empty());
        StructField server_port = new StructField("server_port", DataTypes.IntegerType, true, Metadata.empty());
        StructField filelen = new StructField("filelen", DataTypes.LongType, true, Metadata.empty());
        StructField content_length = new StructField("content_length", DataTypes.LongType, true, Metadata.empty());
        StructField status = new StructField("status", DataTypes.StringType, true, Metadata.empty());
        StructField a1 = new StructField("a1", DataTypes.StringType, true, Metadata.empty());
        StructField a2 = new StructField("a2", DataTypes.StringType, true, Metadata.empty());
        StructField a3 = new StructField("a3", DataTypes.StringType, true, Metadata.empty());
        StructField a4 = new StructField("a4", DataTypes.StringType, true, Metadata.empty());
        StructField a5 = new StructField("a5", DataTypes.StringType, true, Metadata.empty());
        StructField a6 = new StructField("a6", DataTypes.StringType, true, Metadata.empty());
        StructField a7 = new StructField("a7", DataTypes.StringType, true, Metadata.empty());
        StructField a8 = new StructField("a8", DataTypes.StringType, true, Metadata.empty());
        StructField a9 = new StructField("a9", DataTypes.StringType, true, Metadata.empty());
        StructField a10 = new StructField("a10", DataTypes.StringType, true, Metadata.empty());
        StructField a11 = new StructField("a11", DataTypes.StringType, true, Metadata.empty());
        StructField a12 = new StructField("a12", DataTypes.StringType, true, Metadata.empty());
        StructField a13 = new StructField("a13", DataTypes.StringType, true, Metadata.empty());
        StructField a14 = new StructField("a14", DataTypes.StringType, true, Metadata.empty());
        StructField a15 = new StructField("a15", DataTypes.StringType, true, Metadata.empty());
        StructField a16 = new StructField("a16", DataTypes.StringType, true, Metadata.empty());


        StructType structType = new StructType(new StructField[]{id, protocal, protocal_type, protocal_apply, protocal_detail,
                user_ip, user_packet, user_port, server_ip, server_packet, server_port, filelen, content_length, status,
                a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16});

        List<String> ics = new ArrayList<>();
        ics.add("coap");
        ics.add("mqtt");
        ics.add("xmpp");
        ics.add("http");
        ics.add("rest");
        ics.add("soap");
        ics.add("jms");
        ics.add("amqp");
        ics.add("jtext");
        ics.add("jt808");
        ics.add("edp");

        Dataset<Row> rows = sqlContext.read().csv(StringUtils.join(filePaths, ",").split(","));
        JavaRDD<Row> javaRDD = rows.toJavaRDD();

        javaRDD = javaRDD.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                try {
                    String rowString = row.getString(0);
                    String[] rows = rowString.split("\\t+");
                    String id = rows[0];
                    String protocal = rows[1];
                    String protocal_type = "iot";
                    if (ics.contains(protocal.toLowerCase())) {
                        protocal_type = "ics";
                    }
                    String protocal_apply = rows[2];
                    String protocal_detail = rows[3];
                    String user_ip = rows[4];
                    String user_packet = rows[5];
                    int user_port = Integer.parseInt(rows[6]);

                    String server_ip = rows[7];
                    String server_packet = rows[8];
                    int server_port = Integer.parseInt(rows[9]);
                    long filelen = Long.parseLong(rows[10]);
                    long content_length = Long.parseLong(rows[11]);
                    String status = rows[12];

                    List<Object> fields = new ArrayList<>();
                    fields.add(id);
                    fields.add(protocal);
                    fields.add(protocal_type);
                    fields.add(protocal_apply);
                    fields.add(protocal_detail);
                    fields.add(user_ip);
                    fields.add(user_packet);
                    fields.add(user_port);
                    fields.add(server_ip);
                    fields.add(server_packet);
                    fields.add(server_port);
                    fields.add(filelen);
                    fields.add(content_length);
                    fields.add(status);

                    int currentLength = fields.size() + 1;
                    for (int i = currentLength; i < rows.length; i++) fields.add(rows[i]);
                    for (int i = fields.size(); i < structType.length(); i++) fields.add("");
                    return new GenericRow(fields.toArray());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                return new GenericRow();
            }
        });
        javaRDD.collect().forEach(row -> System.out.println(row.mkString(",")));

        Dataset<Row> dataset = sqlContext.createDataFrame(javaRDD, structType);
        dataset.printSchema();
        dataset.show(10);

        /*sqlContext.sql("use ads");
        sqlContext.sql("drop table if exists t_ods_gynet_email_log");
        sqlContext.createExternalTable("t_ods_gynet_email_log", "parquet", structType, new HashMap<>());
        dataset.write().mode(SaveMode.Overwrite).insertInto("t_ods_gynet_email_log");*/
    }
}
