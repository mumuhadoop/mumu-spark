package com.lovecws.mumu.spark.ml;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 机器学习算法
 * @date 2018-02-08 11:15
 */
public class MachineLeaningTest {

    private MachineLeaning machineLeaning = new MachineLeaning();

    @Test
    public void vector() {
        machineLeaning.vector();
    }

    @Test
    public void libsvm() {
        machineLeaning.libsvm();
    }

    @Test
    public void matix() {
        machineLeaning.matix();
    }
}
