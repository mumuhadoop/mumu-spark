package com.lovecws.mumu.spark.streaming;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: spark streaming [nc -lk 9999]
 * @date 2017-10-30 14:33
 */
public class SocketSparkStreaming implements Serializable {

    /**
     * 监听tcp链接
     *
     * @param checkpointDirectory 检查点目录
     * @param batchDuration       间隔
     * @param socketAddress       socket地址
     * @param socketPort          socket端口
     */
    public void streaming(String checkpointDirectory, long batchDuration, String socketAddress, int socketPort) {
        JavaStreamingContext streamingContext = checkpoint(checkpointDirectory,batchDuration);
        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream(socketAddress, socketPort);

        JavaDStream<String> mapStream = dStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(final String line) throws Exception {
                System.out.println(line);
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairStrem = mapStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(final String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        pairStrem.print();

        JavaPairDStream<String, Integer> javaPairDStream = pairStrem.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(final List<Integer> values, final Optional<Integer> state) throws Exception {
                Integer updatedValue = 0;
                if (state.isPresent()) {
                    updatedValue = state.get();
                }
                for (Integer value : values) {
                    updatedValue += value;
                }
                return Optional.of(updatedValue);
            }
        });
        javaPairDStream.print();

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * spark streaming添加检查点
     *
     * @param checkpointDirectory 检查点目录
     * @param batchDuration       批处理间隔
     * @return
     */
    public JavaStreamingContext checkpoint(String checkpointDirectory, long batchDuration) {
        JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(checkpointDirectory, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                JavaStreamingContext streamingContext = new MumuSparkConfiguration().javaStreamingContext(batchDuration);
                streamingContext.checkpoint(checkpointDirectory);
                return streamingContext;
            }
        });
        return streamingContext;
    }
}
