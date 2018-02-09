package com.lovecws.mumu.spark.streaming.advance;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: twitter streaming
 * @date 2017-10-31 9:26
 */
public class TwitterSparkStreaming {

    public void fileStreaming(String textFile, long batchDuration){
        JavaStreamingContext streamingContext = new MumuSparkConfiguration().javaStreamingContext(batchDuration);

    }
}
