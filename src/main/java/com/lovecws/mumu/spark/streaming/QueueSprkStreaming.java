package com.lovecws.mumu.spark.streaming;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 队列流
 * @date 2017-10-31 11:51
 */
public class QueueSprkStreaming implements Serializable {

    private static final Logger log = Logger.getLogger(QueueSprkStreaming.class);

    /**
     * @param batchDuration 间隔
     */
    public void queue(long batchDuration) {
        JavaStreamingContext streamingContext = new MumuSparkConfiguration().javaStreamingContext(batchDuration);

        List<Integer> list = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            list.add(random.nextInt(1000));
        }

        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
        for (int i = 0; i < 30; i++) {
            rddQueue.add(streamingContext.sparkContext().parallelize(list));
        }

        JavaDStream<Integer> inputStream = streamingContext.queueStream(rddQueue);
        JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(
                i -> new Tuple2<>(i % 10, 1));
        JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(
                (i1, i2) -> i1 + i2);

        reducedStream.print();
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
            streamingContext.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static final List<String> list = new ArrayList<String>();

    static {
        list.add("lovercw,babymm,love,cws");
        list.add("youzi,ceshu,love,cws");
        list.add("ssa,babymm,love,wwss");
        list.add("saa,ssa,wwes,wewe");
        list.add("we,babymm,rng,skt");
        list.add("df,dt,lg,tg");
    }

    /**
     * 队列数据流
     *
     * @param batchDuration spark streaming监听间隔
     */
    public void queueStream(long batchDuration) {
        JavaStreamingContext streamingContext = new MumuSparkConfiguration().javaStreamingContext(batchDuration);

        Queue<JavaRDD<String>> queue = new LinkedBlockingQueue<JavaRDD<String>>();
        for (int i = 0; i < 2; i++) {
            List<String> rdd = new ArrayList<String>();
            rdd.add(list.get(new Random().nextInt(list.size())));
            rdd.add(list.get(new Random().nextInt(list.size())));
            JavaRDD<String> javaRDD = streamingContext.sparkContext().parallelize(rdd);
            queue.add(javaRDD);
        }
        JavaDStream<String> queueStream = streamingContext.queueStream(queue, true);
        queueStream.print();

        JavaDStream<String> mapDStream = queueStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(final String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pairDStream = mapDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(final String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> reduceDStream = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer integer, final Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        reduceDStream.print();
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
            streamingContext.close();
        } catch (InterruptedException e) {
            log.error(e);
        }
    }
}
