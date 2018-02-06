package com.lovecws.mumu.spark.rdd.wordcount;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 文本字数统计
 * @date 2018-02-05 15:15
 */
public class SparkWordCount implements Serializable {

    public void wordcount(String filepath) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<String> javaRDD = sparkContext.textFile(filepath);

        Accumulator<Integer> accumulator = sparkContext.accumulator(0);

        JavaPairRDD<String, Integer> flatMapToPair = javaRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(final String readline) throws Exception {
                if (readline == null) {
                    return null;
                }
                String[] splits = readline.split(" ");
                List<Tuple2<String, Integer>> tuples = new ArrayList<Tuple2<String, Integer>>();
                for (String split : splits) {
                    Tuple2<String, Integer> tuple2 = new Tuple2<>(split, 1);
                    tuples.add(tuple2);
                    accumulator.add(1);
                }
                return tuples.iterator();
            }
        });
        System.out.println("flatMap:" + StringUtils.join(flatMapToPair.collect()));

        flatMapToPair.persist(StorageLevel.MEMORY_ONLY());

        Map<String, Long> stringLongMap = flatMapToPair.countByKey();
        System.out.println("countByKey:" + stringLongMap);

        System.out.println("accumulator:" + accumulator.value());
    }
}
