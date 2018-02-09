package com.lovecws.mumu.spark.rdd.accumulator;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 我的向量
 * @date 2018-02-09 12:45
 */
public class MyVector {

    private int x;
    private int y;
    private int value;

    public MyVector() {
    }

    public MyVector(final int x, final int y, final int value) {
        this.x = x;
        this.y = y;
        this.value = value;
    }

    public int getX() {
        return x;
    }

    public void setX(final int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(final int y) {
        this.y = y;
    }

    public int getValue() {
        return value;
    }

    public void setValue(final int value) {
        this.value = value;
    }
}
