package com.lovecws.mumu.spark.ml.transformer;

import com.lovecws.mumu.spark.ml.transformer.MachineLeaningTransformer;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 机器学习数据转换
 * @date 2018-02-07 17:15
 */
public class MachineLeaningTransformerTest {

    private MachineLeaningTransformer transformer = new MachineLeaningTransformer();

    @Test
    public void tokenizer() {
        transformer.tokenizer();
    }

    @Test
    public void stopWordsRemover() {
        transformer.stopWordsRemover();
    }

    @Test
    public void ngram() {
        transformer.ngram();
    }

    @Test
    public void binarizer() {
        transformer.binarizer();
    }

    @Test
    public void pca() {
        transformer.pca();
    }

    @Test
    public void polynomialExpansion() {
        transformer.polynomialExpansion();
    }

    @Test
    public void dct() {
        transformer.dct();
    }

    @Test
    public void stringIndexer() {
        transformer.stringIndexer();
    }

    @Test
    public void IndexToString() {
        transformer.IndexToString();
    }

    @Test
    public void OneHotEncoder() {
        transformer.OneHotEncoder();
    }

    @Test
    public void vectorIndexer() {
        transformer.vectorIndexer();
    }

    @Test
    public void interaction() {
        transformer.interaction();
    }

    @Test
    public void Normalizer() {
        transformer.Normalizer();
    }

    @Test
    public void StandardScaler() {
        transformer.StandardScaler();
    }

    @Test
    public void MinMaxScaler() {
        transformer.MinMaxScaler();
    }

    @Test
    public void MaxAbsScaler() {
        transformer.MaxAbsScaler();
    }

    @Test
    public void Bucketizer() {
        transformer.Bucketizer();
    }

    @Test
    public void ElementwiseProduct() {
        transformer.ElementwiseProduct();
    }

    @Test
    public void SQLTransformer() {
        transformer.SQLTransformer();
    }

    @Test
    public void VectorAssembler() {
        transformer.VectorAssembler();
    }

    @Test
    public void QuantileDiscretizer() {
        transformer.QuantileDiscretizer();
    }

    @Test
    public void Imputer() {
        transformer.Imputer();
    }
}
