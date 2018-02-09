package com.lovecws.mumu.spark.graphx;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.impl.EdgeRDDImpl;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: graphx图计算框架
 * @date 2017-11-02 10:09
 */
public class SparkGraphx {

    public static void main(String[] args) {
        new SparkGraphx().main();
        new SparkGraphx().createGraph();
    }

    public void main() {
        SQLContext spark = new MumuSparkConfiguration().sqlContext();
        Graph<Object, Object> graph = GraphLoader.edgeListFile(spark.sparkContext(), "data/graphx/followers.txt", false, -1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY());
        System.out.println(JSON.toJSONString(graph));

        EdgeRDD<Object> edges = graph.edges();
        System.out.println(JSON.toJSONString(edges));

        VertexRDD<Object> vertices = graph.vertices();
        System.out.println(JSON.toJSONString(vertices));

        RDD<EdgeTriplet<Object, Object>> triplets = graph.triplets();
        System.out.println(JSON.toJSONString(triplets));
    }

    public void createGraph() {
        JavaSparkContext sparkContext = new MumuSparkConfiguration().javaSparkContext();
        List<Edge<String>> edgeList = new ArrayList<Edge<String>>();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 20; j++) {
                Edge<String> edge = new Edge<String>(i, j, "ij" + i + j);
                edgeList.add(edge);
            }
        }
        JavaRDD<Edge<String>> edgeJavaRDD = sparkContext.parallelize(edgeList);
        EdgeRDDImpl<String, Object> edgeRDD = EdgeRDD.fromEdges(edgeJavaRDD.rdd(), null, null);
    }
}