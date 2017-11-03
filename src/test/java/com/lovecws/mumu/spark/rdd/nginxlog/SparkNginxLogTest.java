package com.lovecws.mumu.spark.rdd.nginxlog;

import com.lovecws.mumu.spark.rdd.nginxlog.SparkNginxLog;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: nginxlog日志统计测试
 * @date 2017-11-01 9:24
 */
public class SparkNginxLogTest {
    private SparkNginxLog sparkNginxLog = new SparkNginxLog();
    private static final String HADFS_TEXTFILE = "hdfs://192.168.11.25:9000/mapreduce/nginxlog/access/input";

    @Test
    public void dairyAccessCount() {
        sparkNginxLog.accessCount(HADFS_TEXTFILE, "yyyy-MM-dd");
    }

    @Test
    public void monthAccessCount() {
        sparkNginxLog.accessCount(HADFS_TEXTFILE, "yyyy-MM");
    }

    @Test
    public void yearAccessCount() {
        sparkNginxLog.accessCount(HADFS_TEXTFILE, "yyyy");
    }

    @Test
    public void hourAccessCount() {
        sparkNginxLog.accessCount(HADFS_TEXTFILE, "HH");
    }

    @Test
    public void dairyIPcount() {
        sparkNginxLog.ipcount(HADFS_TEXTFILE, "yyyy-MM-dd");
    }

    @Test
    public void monthIPcount() {
        sparkNginxLog.ipcount(HADFS_TEXTFILE, "yyyy-MM");
    }

    @Test
    public void yearIPcount() {
        sparkNginxLog.ipcount(HADFS_TEXTFILE, "yyyy");
    }

    @Test
    public void hourIPcount() {
        sparkNginxLog.ipcount(HADFS_TEXTFILE, "HH");
    }

    @Test
    public void dairyJOIN() {
        sparkNginxLog.join(HADFS_TEXTFILE, "yyyy-MM-dd");
    }

    @Test
    public void monthJOIN() {
        sparkNginxLog.join(HADFS_TEXTFILE, "yyyy-MM");
    }

    @Test
    public void yearJOIN() {
        sparkNginxLog.join(HADFS_TEXTFILE, "yyyy");
    }

    @Test
    public void hourJOIN() {
        sparkNginxLog.join(HADFS_TEXTFILE, "HH");
    }
}
