package com.lovecws.mumu.spark.sql.streaming;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: socket structedStreaming
 * @date 2018-02-07 13:56
 */
public class SocketStructedStreaming {

    public void fileStreaming(String socketAddress, int socketPort) {
        SQLContext sqlContext = new MumuSparkConfiguration().sqlContext();
        Dataset<Row> lines = sqlContext
                .readStream()
                .format("socket")
                .option("host", socketAddress)
                .option("port", socketPort)
                .load();
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

}
