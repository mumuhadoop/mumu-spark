package com.lovecws.mumu.spark.streaming;

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

    /**
     * nc -lk 9999
     */
    @Test
    public void streaming() {
        sparkStreamingOperation.streaming("E:\\mumu\\spark\\streaming\\socket", 10l, "192.168.11.25", 9999);
    }

    @Test
    public void checkpoint(){
        JavaStreamingContext checkpoint = sparkStreamingOperation.checkpoint("E:\\mumu\\spark\\streaming\\socketcheckpoint", 10l);
        System.out.println(checkpoint);
    }

    @Test
    public void socketReceiver() {
        sparkStreamingOperation.socketReceiver("E:\\mumu\\spark\\streaming\\socketcheckpoint", 10l, "192.168.11.25", 9999);
    }

}
