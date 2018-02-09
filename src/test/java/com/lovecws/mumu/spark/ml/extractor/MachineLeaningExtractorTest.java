package com.lovecws.mumu.spark.ml.extractor;

import com.lovecws.mumu.spark.ml.extractor.MachineLeaningExtractor;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: tf-idf测试
 * @date 2018-02-07 16:50
 */
public class MachineLeaningExtractorTest {

    private MachineLeaningExtractor extractor = new MachineLeaningExtractor();

    @Test
    public void tfidf() {
        extractor.tfidf();
    }

    @Test
    public void word2Vec() {
        extractor.word2Vec();
    }

    @Test
    public void countVectorizer() {
        extractor.countVectorizer();
    }
}
