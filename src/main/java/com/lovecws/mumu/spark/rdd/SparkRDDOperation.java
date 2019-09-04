package com.lovecws.mumu.spark.rdd;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import com.lovecws.mumu.spark.rdd.accumulator.MyVector;
import com.lovecws.mumu.spark.rdd.accumulator.MyVectorAccumulator;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: rdd基本操作
 * @date 2017-10-30 13:32
 */
public class SparkRDDOperation {

    /**
     * 计算rdd集合数量
     *
     * @param list
     */
    public void count(List<Object> list) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<Object> javaRDD = sparkContext.parallelize(list);
        javaRDD.persist(StorageLevel.MEMORY_ONLY());
        javaRDD.cache();
        System.out.println("count " + javaRDD.count());
        System.out.println("javaRDD " + StringUtils.join(javaRDD.collect()));
        sparkContext.close();
    }

    /**
     * 计算rdd集合第一个元素
     *
     * @param list
     */
    public void first(List<Object> list) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<Object> javaRDD = sparkContext.parallelize(list);
        System.out.println("first " + javaRDD.first());
        System.out.println("javaRDD " + StringUtils.join(javaRDD.collect()));
        sparkContext.close();
    }

    /**
     * 计算rdd集合前多少个元素
     *
     * @param list
     */
    public void take(List<Object> list, int elementCount) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<Object> javaRDD = sparkContext.parallelize(list);
        System.out.println("take " + javaRDD.take(elementCount));
        sparkContext.close();
    }

    /**
     * 从文件中读取记录
     */
    public void textFile(String filePath) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        sparkContext.addFile(filePath,true);
        JavaRDD<String> javaRDD = sparkContext.textFile(filePath);
        //缓存数据集
        javaRDD.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(javaRDD.count());
        List<String> collect = javaRDD.collect();
        for (String str : collect) {
            System.out.println(str);
        }
        sparkContext.close();
    }

    /**
     * 从文件中读取记录
     */
    public void wholeTextFiles(String filePath) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaPairRDD<String, String> javaPairRDD = sparkContext.wholeTextFiles(filePath, 2);
        //缓存数据集
        javaPairRDD.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(javaPairRDD.count());
        List<Tuple2<String, String>> collect = javaPairRDD.collect();
        for (Tuple2 str : collect) {
            System.out.println(str._1 + "_" + str._2);
        }
        sparkContext.close();
    }

    /**
     * 广播变量 当一些变量多个节点都要使用的时候 可以将这个变量通过广播的形式来共享 从而解决数据传输效率问题
     */
    public void broadcast() {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        Broadcast<String> broadcast = sparkContext.broadcast("10");
        String value = broadcast.getValue();
        System.out.println(value);
        sparkContext.close();
    }

    /**
     * 累加器
     */
    public void accumulator() {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        Accumulator<Integer> accumulator = sparkContext.accumulator(10);
        accumulator.add(100);
        System.out.println(accumulator.value());
        sparkContext.close();

        sparkContext.sc().register(new MyVectorAccumulator(), "vectorAccumulator");
        Accumulator<MyVector> vectorAccumulator = sparkContext.accumulator(new MyVector(), new AccumulatorParam<MyVector>() {
            @Override
            public MyVector addAccumulator(final MyVector vector, final MyVector t1) {
                return null;
            }

            @Override
            public MyVector addInPlace(final MyVector vector, final MyVector r1) {
                return null;
            }

            @Override
            public MyVector zero(final MyVector vector) {
                return null;
            }
        });
        System.out.println(vectorAccumulator);
    }

    public void saveAsObjectFile(List<Object> list) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<Object> javaRDD = sparkContext.parallelize(list);
        javaRDD.saveAsObjectFile("D:\\data\\spark\\object");
        sparkContext.close();
    }

    /**
     * 以text的形式保存文件
     *
     * @param list
     */
    public void saveAsTextFile(List<Object> list) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<Object> javaRDD = sparkContext.parallelize(list);
        javaRDD.saveAsTextFile("D:\\data\\spark\\text");
        sparkContext.close();
    }
}
