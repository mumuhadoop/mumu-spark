package com.lovecws.mumu.spark.streaming;

import com.lovecws.mumu.spark.streaming.SocketSparkStreaming;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: spark streaming
 * @date 2017-10-30 14:45
 */
public class SocketSparkStreamingTest {

    private SocketSparkStreaming sparkStreamingOperation = new SocketSparkStreaming();

    @Test
    public void streaming() {
        sparkStreamingOperation.streaming("D:\\data\\sparkstreaming\\checkpoint\\socket", 10l, "192.168.11.25", 9999);
    }

    @Test
    public void checkpoint(){
        JavaStreamingContext checkpoint = sparkStreamingOperation.checkpoint("D:\\data\\sparkstreaming\\checkpoint\\socket", 10l);
        System.out.println(checkpoint);
    }
}
