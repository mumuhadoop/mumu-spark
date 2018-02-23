package com.lovecws.mumu.spark.streaming;

import akka.actor.Props;
import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.akka.AkkaUtils;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: akka actor
 * @date 2017-10-31 9:49
 */
public class AkkaActorSparkStreaming implements Serializable{

    public void akka(String checkpointDirectory, long batchDuration) {
        try {
            FileUtils.deleteDirectory(new File(checkpointDirectory));
        } catch (IOException e) {
            e.printStackTrace();
        }
        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointDirectory, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                JavaStreamingContext streamingContext = new MumuSparkConfiguration().javaStreamingContext(batchDuration);
                streamingContext.checkpoint(checkpointDirectory);
                return streamingContext;
            }
        });
        JavaReceiverInputDStream<Object> akkaStream = AkkaUtils.createStream(javaStreamingContext, Props.empty(), "sparkAkkaStreaming");
        JavaPairDStream<String, Integer> mapDStream = akkaStream.flatMapToPair(new PairFlatMapFunction<Object, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Object object) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                String[] strings = object.toString().split(" ");
                for (String str : strings) {
                    list.add(new Tuple2<>(str, 1));
                }
                return list.iterator();
            }
        });
        JavaPairDStream<String, Integer> reduceDStream = mapDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairDStream<String, Integer> stateDStream = reduceDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
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
        stateDStream.print();
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new AkkaActorSparkStreaming().akka("E:\\mumu\\spark\\streaming\\akkacheckpoint",10l);
    }
}
