package com.lovecws.mumu.spark.rdd;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

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
    }

    public void saveAsObjectFile(List<Object> list) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<Object> javaRDD = sparkContext.parallelize(list);
        javaRDD.saveAsObjectFile("D:\\data\\spark\\object");
        sparkContext.close();
    }

    /**
     * 以text的形式保存文件
     * @param list
     */
    public void saveAsTextFile(List<Object> list) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<Object> javaRDD = sparkContext.parallelize(list);
        javaRDD.saveAsTextFile("D:\\data\\spark\\text");
        sparkContext.close();
    }
}
