package com.lovecws.mumu.spark.ml;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.ml.linalg.Matrices;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import scala.collection.Iterator;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: spark 机器学习语言
 * @date 2018-02-08 11:04
 */
public class MachineLeaning {

    public void vector() {
        //密集向量
        Vector denseVector = Vectors.dense(4.0, 5, 0, 3.0);
        System.out.println(denseVector);

        //稀疏向量
        Vector sparseVector = Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, 4.0});
        System.out.println(sparseVector);
    }

    public void libsvm() {
        SQLContext spark = new MumuSparkConfiguration().sqlContext();
        RDD<LabeledPoint> labeledPointRDD = MLUtils.loadLibSVMFile(spark.sparkContext(), "data/mllib/sample_lda_libsvm_data.txt");

        Iterator<LabeledPoint> labeledPointIterator = labeledPointRDD.toLocalIterator();
        while (labeledPointIterator.hasNext()) {
            LabeledPoint labeledPoint = labeledPointIterator.next();
            System.out.println(JSON.toJSONString(labeledPoint));
        }

        RDD<LabeledPoint>[] randomSplit = labeledPointRDD.randomSplit(new double[]{0.6, 0.4}, 11l);
    }

    public void matix() {
        //密集矩阵
        Matrix denseMatrix = Matrices.dense(3, 2, new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0});
        System.out.println("denseMatrix:\n" + denseMatrix);

        //稀疏矩阵
        System.out.println();
        Matrix sparseMatrix = Matrices.sparse(3, 2, new int[]{0, 1, 3}, new int[]{0, 1, 2}, new double[]{1.0, 2.0, 3.0});
        System.out.println("sparseMatrix:\n" + sparseMatrix);

        //行级分布式矩阵
        RowMatrix rowMatrix = new RowMatrix(null);

        //索引行级矩阵
        IndexedRowMatrix indexedRowMatrix = new IndexedRowMatrix(null);

        //坐标矩阵
        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(null);

        //块矩阵
        BlockMatrix blockMatrix = coordinateMatrix.toBlockMatrix();
    }
}
