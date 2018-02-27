package com.lovecws.mumu.spark.streaming;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: akka测试
 * @date 2018-02-27 8:59
 */
public class AkkaActorSparkStreamingTest {

    private AkkaActorSparkStreaming akkaActorSparkStreaming=new AkkaActorSparkStreaming();

    @Test
    public void akkaStreaming(){
        akkaActorSparkStreaming.akkaStreaming("E:\\mumu\\spark\\streaming\\akkacheckpoint", 10l);
    }

    @Test
    public void startAkka(){
        akkaActorSparkStreaming.startAkka();
    }
}
