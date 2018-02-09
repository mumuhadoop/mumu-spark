package com.lovecws.mumu.spark.streaming;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 文件sparkstreaming测试
 * @date 2017-10-30 16:36
 */
public class FileSparkStreamingTest {

    private static final Logger log = Logger.getLogger(FileSparkStreamingTest.class);

    private FileSparkStreaming sparkStreaming = new FileSparkStreaming();

    @Test
    public void streaming() {
        sparkStreaming.fileStreaming("E:\\mumu\\spark\\streaming\\file", 10l);
    }

    @Test
    public void hadoop() {
        sparkStreaming.fileStreaming("hdfs://192.168.11.25:9000/mumu/spark/file", 10l);
    }

    @Test
    public void fileStreamingState() {
        sparkStreaming.fileStreamingState("E:\\mumu\\spark\\streaming\\filecheckpoint", "E:\\mumu\\spark\\streaming\\file", 10l);
    }
}
