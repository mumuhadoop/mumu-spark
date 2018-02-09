package com.lovecws.mumu.spark.rdd.accumulator;

import org.apache.spark.util.AccumulatorV2;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 自定义累加器
 * @date 2018-02-09 12:20
 */
public class MyVectorAccumulator extends AccumulatorV2<MyVector, MyVector> {

    private MyVector myVector = new MyVector();
    private List<MyVector> vectors = new ArrayList<MyVector>();

    @Override
    public boolean isZero() {
        return vectors.isEmpty();
    }

    @Override
    public AccumulatorV2 copy() {
        MyVectorAccumulator myVectorAccumulator = new MyVectorAccumulator();
        myVectorAccumulator.myVector = myVector;
        myVectorAccumulator.vectors = vectors;
        return myVectorAccumulator;
    }

    @Override
    public void reset() {
        myVector = new MyVector();
        vectors.clear();
    }

    @Override
    public void add(final MyVector vector) {
        myVector = vector;
        vectors.add(myVector);
    }

    @Override
    public void merge(final AccumulatorV2 accumulatorV2) {
        if (accumulatorV2 instanceof MyVectorAccumulator) {
            MyVectorAccumulator myVectorAccumulator = (MyVectorAccumulator) accumulatorV2;
            myVector = myVectorAccumulator.myVector;
            vectors = myVectorAccumulator.vectors;
            BoxedUnit var4 = BoxedUnit.UNIT;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public MyVector value() {
        return myVector;
    }
}
