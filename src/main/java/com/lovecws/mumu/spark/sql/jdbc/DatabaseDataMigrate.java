package com.lovecws.mumu.spark.sql.jdbc;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: mumu-spark
 * @description: 数据库数据迁移，将数据库的表数据迁移到另外一个表中
 * @author: 甘亮
 * @create: 2019-10-18 14:40
 **/
public class DatabaseDataMigrate implements Serializable {

    private SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();

    public void pg2gbase(String table) {
        //输入表
        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "postgres");
        properties.put("driver", "org.postgresql.Driver");
        Dataset<Row> dataset = sqlContext.read().jdbc("jdbc:postgresql://172.31.134.225:5432/ableads", table, properties);
        dataset.printSchema();
        dataset.show(10);

        //输出表
        /*
            Properties outProperties = new Properties();
            outProperties.put("user", "sysdba");
            outProperties.put("password", "GBase8sV8316");
            outProperties.put("driver", "com.gbase.jdbc.Driver");
            dataFrame.write().mode(SaveMode.Append).jdbc("jdbc:gbase://172.31.134.249:5258/ads", table + "_2", outProperties);
        */
        try {
            Class.forName("com.gbase.jdbc.Driver");

            //创建表
            StructField[] fields = dataset.schema().fields();

            //数据录入
            dataset.foreachPartition(new ForeachPartitionFunction<Row>() {
                @Override
                public void call(Iterator<Row> iterator) throws Exception {
                    Connection connection = DriverManager.getConnection("jdbc:gbase://172.31.134.249:5258/test", "sysdba", "GBase8sV8316");
                    PreparedStatement preparedStatement = null;
                    AtomicInteger counter = new AtomicInteger(0);

                    AtomicBoolean init = new AtomicBoolean(false);
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        String[] fieldNames = row.schema().fieldNames();
                        if (!init.get()) {
                            init.set(true);

                            List<Object> columns = new ArrayList<>();
                            List<Object> dots = new ArrayList<>();

                            for (int i = 0; i < fieldNames.length; i++) {
                                dots.add("?");
                                String fieldName = fieldNames[i];
                                if ("describe".equalsIgnoreCase(fieldName)) fieldName = "`" + fieldName + "`";
                                columns.add(fieldName);
                            }
                            String sql = "insert into " + table + "(" + StringUtils.join(columns, ",") + ") values(" + StringUtils.join(dots, ",") + ")";
                            preparedStatement = connection.prepareStatement(sql);
                        }
                        for (int i = 0; i < fieldNames.length; i++) {
                            Object value = row.get(i);
                            if (fieldNames[i].equalsIgnoreCase("create_time")) {
                                value = new Date();
                            }
                            preparedStatement.setObject(i + 1, value);
                        }
                        preparedStatement.addBatch();

                        counter.addAndGet(1);
                        if (counter.get() > 1000) {
                            preparedStatement.executeBatch();
                            preparedStatement.clearBatch();
                            counter.set(0);
                        }
                    }
                    if (counter.get() > 0) preparedStatement.executeBatch();
                    connection.close();
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
