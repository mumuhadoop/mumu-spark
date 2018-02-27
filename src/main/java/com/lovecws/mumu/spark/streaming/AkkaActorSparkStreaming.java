package com.lovecws.mumu.spark.streaming;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.lovecws.mumu.spark.MumuSparkConfiguration;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.akka.AkkaUtils;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: akka actor
 * @date 2017-10-31 9:49
 */
public class AkkaActorSparkStreaming implements Serializable{

    public void akkaStreaming(String checkpointDirectory, long batchDuration) {
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

    /**
     * 开始发送actor消息
     */
    public void startAkka() {
        ActorSystem actorSystem = ActorSystem.create("streaming-actor-system-0", ConfigFactory.load("akka/client.conf"));
        final ActorRef myAkkaClusterClient = actorSystem.actorOf(Props.create(AkkaClusterClient.class), "myAkkaClusterClient");
        myAkkaClusterClient.tell("lovecws" + DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:SS"), ActorRef.noSender());
    }

    public static class AkkaClusterClient extends UntypedActor {
        @Override
        public void onReceive(Object message) throws Throwable {
            System.out.println(message);
        }
    }
}
