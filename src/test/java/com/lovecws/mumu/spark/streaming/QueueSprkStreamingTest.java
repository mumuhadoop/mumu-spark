package com.lovecws.mumu.spark.streaming;

import com.lovecws.mumu.spark.streaming.QueueSprkStreaming;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-31 12:29
 */
public class QueueSprkStreamingTest {

    private QueueSprkStreaming queueSprkStreaming = new QueueSprkStreaming();

    @Test
    public void queue() {
        queueSprkStreaming.queue(10l);
    }

    @Test
    public void queueStream() {
        queueSprkStreaming.queueStream(10l);
    }
}
