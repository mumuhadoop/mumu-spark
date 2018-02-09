package com.lovecws.mumu.spark.sql.streaming;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: socket 数据流
 * @date 2018-02-07 13:58
 */
public class SocketStructedStreamingTest {

    @Test
    public void structedStreaming() {
        new SocketStructedStreaming().fileStreaming("192.168.11.25", 9999);
    }
}
