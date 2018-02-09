package com.lovecws.mumu.spark.ml.selector;

import com.lovecws.mumu.spark.ml.selector.MachineLeaningSelector;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 选择器
 * @date 2018-02-08 9:10
 */
public class MachineLeaningSelectorTest {

    private MachineLeaningSelector selector = new MachineLeaningSelector();

    @Test
    public void selectors() {
        selector.selectors();
    }

    @Test
    public void rformula() {
        selector.rformula();
    }

    @Test
    public void chisqselector() {
        selector.chisqselector();
    }

    @Test
    public void lsh() {
        selector.lsh();
    }

    @Test
    public void minhash() {
        selector.minhash();
    }
}
