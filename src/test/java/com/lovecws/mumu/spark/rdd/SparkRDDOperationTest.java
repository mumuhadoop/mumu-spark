package com.lovecws.mumu.spark.rdd;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: spark rdd基本操作
 * @date 2017-10-30 13:45
 */
public class SparkRDDOperationTest {

    private SparkRDDOperation sparkRDDOperation = new SparkRDDOperation();

    @Test
    public void count() {
        List<Object> list = new ArrayList<Object>();
        list.add("lovecws");
        list.add("cws");
        list.add("cws");
        list.add("mumu");
        sparkRDDOperation.count(list);
    }

    @Test
    public void first() {
        List<Object> list = new ArrayList<Object>();
        list.add("lovecws");
        list.add("cws");
        list.add("cws");
        list.add("mumu");
        sparkRDDOperation.first(list);
    }

    @Test
    public void take() {
        List<Object> list = new ArrayList<Object>();
        list.add("lovecws");
        list.add("cws");
        list.add("cws");
        list.add("mumu");
        sparkRDDOperation.take(list, 2);
    }

    @Test
    public void textFile() {
        sparkRDDOperation.textFile("E:/data/hive/min=89");
    }

    @Test
    public void wholeTextFiles() {
        sparkRDDOperation.wholeTextFiles("E:\\data\\hive");
    }

    @Test
    public void broadcast() {
        sparkRDDOperation.broadcast();
    }

    @Test
    public void accumulator() {
        sparkRDDOperation.accumulator();
    }

    @Test
    public void saveAsObjectFile() {
        List<Object> list = new ArrayList<Object>();
        list.add("lovecws");
        list.add("cws");
        list.add("cws");
        list.add("mumu");
        sparkRDDOperation.saveAsObjectFile(list);
    }

    @Test
    public void saveAsTextFile() {
        List<Object> list = new ArrayList<Object>();
        list.add("lovecws");
        list.add("cws");
        list.add("cws");
        list.add("mumu");
        sparkRDDOperation.saveAsTextFile(list);
    }
}
