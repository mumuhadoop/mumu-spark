package com.lovecws.mumu.spark.streaming.advance;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kafka消息数据流
 * @date 2017-10-31 9:25
 */
public class kafkaSparkStreaming {

    /**
     * 读取kafka主题获取数据
     *
     * @param brokers
     * @param topics
     */
    public void kafka(String brokers, String topics) {
        JavaStreamingContext streamingContext = new MumuSparkConfiguration().javaStreamingContext(10);
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        JavaPairInputDStream<Long, String> directStream = KafkaUtils.createDirectStream(streamingContext, Long.class, String.class, null, null, kafkaParams, topicsSet);
        JavaPairDStream<String, Integer> dStream = directStream.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, String>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(final Tuple2<Long, String> tuple2) throws Exception {
                List<Tuple2<String, Integer>> tuple2List = new ArrayList<Tuple2<String, Integer>>();
                String[] splits = tuple2._2().toString().split(" ");
                for (String word : splits) {
                    tuple2List.add(new Tuple2<>(word, 1));
                }
                return tuple2List.iterator();
            }
        });
        JavaPairDStream<String, Integer> reducedStream = dStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer integer, final Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        reducedStream.print();
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
