package com.lovecws.mumu.spark.rdd.nginxlog;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 使用spark解析nginxlog日志
 * @date 2017-11-01 9:04
 */
public class SparkNginxLog implements Serializable {

    /**
     * nginxlog访问量
     *
     * @param textFile   文件目录
     * @param dateFormat 统计日志日期格式 yyyy-MM-dd、yyyy-MM、yyyy、HH
     */
    public void accessCount(String textFile, String dateFormat) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<String> javaRDD = sparkContext.textFile(textFile);

        JavaPairRDD<String, Integer> mapRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(final String line) throws Exception {
                Map<String, Object> nginxLogMap = NginxAccessLogParser.parseLine(line);
                Object accessTime = nginxLogMap.get("accessTime");
                return new Tuple2<String, Integer>(DateFormatUtils.format((Date) accessTime, dateFormat), 1);
            }
        });
        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer integer, final Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        reduceRDD = reduceRDD.sortByKey(true);
        System.out.println(dateFormat + " 访问量统计结果:");
        for (Tuple2<String, Integer> tuple2 : reduceRDD.collect()) {
            System.out.println(tuple2._1 + "\t" + tuple2._2);
        }
        sparkContext.close();
    }

    /**
     * 统计用户量
     *
     * @param textFile   文件目录
     * @param dateFormat 统计日志日期格式 yyyy-MM-dd、yyyy-MM、yyyy、HH
     */
    public void ipcount(String textFile, String dateFormat) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<String> javaRDD = sparkContext.textFile(textFile);

        JavaPairRDD<String, String> mapRDD = javaRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(final String line) throws Exception {
                Map<String, Object> nginxLogMap = NginxAccessLogParser.parseLine(line);
                Object accessTime = nginxLogMap.get("accessTime");
                Object remoteAddr = nginxLogMap.get("remoteAddr");
                return new Tuple2<String, String>(DateFormatUtils.format((Date) accessTime, dateFormat), remoteAddr.toString());
            }
        });
        //去除重复数据
        mapRDD = mapRDD.distinct();
        Map<String, Long> countMap = mapRDD.countByKey();
        SortedMap<String, Long> sortedMap = new TreeMap<String, Long>(new Comparator<String>() {
            @Override
            public int compare(final String o1, final String o2) {
                return o1.compareTo(o2);
            }
        });
        sortedMap.putAll(countMap);
        for (Map.Entry entry : sortedMap.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }
        sparkContext.close();
    }

    /**
     * join 用户量和请求量
     *
     * @param textFile   文件目录
     * @param dateFormat 统计日志日期格式 yyyy-MM-dd、yyyy-MM、yyyy、HH
     */
    public void join(String textFile, String dateFormat) {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        JavaRDD<String> javaRDD = sparkContext.textFile(textFile);
        javaRDD.cache();

        //计算ip访问量
        JavaPairRDD<String, String> ipCountMapRDD = javaRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(final String line) throws Exception {
                Map<String, Object> nginxLogMap = NginxAccessLogParser.parseLine(line);
                Object accessTime = nginxLogMap.get("accessTime");
                Object remoteAddr = nginxLogMap.get("remoteAddr");
                return new Tuple2<String, String>(DateFormatUtils.format((Date) accessTime, dateFormat), remoteAddr.toString());
            }
        });
        ipCountMapRDD = ipCountMapRDD.distinct();
        JavaPairRDD<String, Integer> ipCountPairRDD = ipCountMapRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(final Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Tuple2<String, Integer>(stringStringTuple2._1, 1);
            }
        });
        JavaPairRDD<String, Integer> ipCountRDD = ipCountPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer integer, final Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        ipCountRDD = ipCountRDD.sortByKey(true);

        //计算请求量
        JavaPairRDD<String, Integer> accessCountMapRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(final String line) throws Exception {
                Map<String, Object> nginxLogMap = NginxAccessLogParser.parseLine(line);
                Object accessTime = nginxLogMap.get("accessTime");
                return new Tuple2<String, Integer>(DateFormatUtils.format((Date) accessTime, dateFormat), 1);
            }
        });
        JavaPairRDD<String, Integer> accessCountReduceRDD = accessCountMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer integer, final Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        accessCountReduceRDD=accessCountReduceRDD.sortByKey(true);

        //join
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = ipCountRDD.join(accessCountReduceRDD);
        joinRDD.sortByKey(true);

        System.out.println(dateFormat + " 统计结果:");
        for (Tuple2<String, Tuple2<Integer, Integer>> tuple2 : joinRDD.collect()) {
            System.out.println(tuple2._1 + "\t" + tuple2._2()._1 + "\t" + tuple2._2()._2);
        }
    }
}
