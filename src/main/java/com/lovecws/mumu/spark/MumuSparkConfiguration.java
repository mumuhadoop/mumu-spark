package com.lovecws.mumu.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: spark quick start
 * @date 2017-10-27 15:43
 */
public class MumuSparkConfiguration {

    private static JavaSparkContext sparkContext = null;

    private static String SPARK_MASTER = "local[2]";
    //private static String SPARK_MASTER = "spark://192.168.11.25:7077";
    //private static String SPARK_MASTER = "yarn";
    private static String HADOOP_URL = "hdfs://192.168.11.25:9000";
    //private static String HADOOP_URL = "local";

    public synchronized JavaSparkContext javaSparkContext() {
        if (sparkContext == null) {
            String master = getMaster();
            SparkConf conf = new SparkConf();
            conf.setAppName("mumuSpark");
            conf.setMaster(master);
            conf.set("spark.streaming.receiver.writeAheadLogs.enable", "true");
            conf.set("spark.driver.allowMultipleContexts", "true");
            sparkContext = new JavaSparkContext(conf);
            if (!master.contains("local")) {
                sparkContext.addJar(hadoopAddress() + "/mumu/spark/jar/mumu-spark.jar");
            }
            if (master.contains("yarn") && System.getenv("HADOOP_CONF_DIR") != null && System.getenv("YARN_CONF_DIR") != null) {
                conf.set("HADOOP_CONF_DIR","D:\\hadoop");
            }
        }
        return sparkContext;
    }

    public synchronized JavaStreamingContext javaStreamingContext(long batchDuration) {
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext(), Durations.seconds(batchDuration));
        return streamingContext;
    }

    public synchronized SQLContext sqlContext() {
        String userDir = System.getProperty("user.dir");
        SparkSession sparkSession = SparkSession
                .builder()
                .master(getMaster())
                .appName("mumuSpark")
                .config("spark.sql.warehouse.dir", userDir + File.separator + "spark-warehouse")
                .config("spark.driver.allowMultipleContexts", true)
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        return sqlContext;
    }

    public synchronized SQLContext hiveContext() {
        String userDir = System.getProperty("user.dir");
        SparkSession sparkSession = SparkSession
                .builder()
                .master(getMaster())
                .appName("mumuSpark")
                .config("spark.sql.warehouse.dir", userDir + File.separator + "hive-warehouse")
                .config("spark.driver.allowMultipleContexts", true)
                .enableHiveSupport()
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        return sqlContext;
    }

    public String getMaster() {
        String spark_master = System.getenv("SPARK_MASTER");
        if (spark_master != null && !"".equals(spark_master)) {
            SPARK_MASTER = spark_master;
        }
        return SPARK_MASTER;
    }

    public String hadoopAddress() {
        String hadoop_url = System.getenv("HADOOP_URL");
        if (hadoop_url != null && !"".equals(HADOOP_URL)) {
            HADOOP_URL = hadoop_url;
        }
        return HADOOP_URL;
    }

    /**
     * 上传jar包
     */
    public void uploadJar() {
        Configuration configuration = new Configuration();
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        try {
            distributedFileSystem.initialize(new URI(hadoopAddress()), configuration);
            FileStatus[] fileStatuses = distributedFileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus);
            }
            String userDir = System.getProperty("user.dir");
            distributedFileSystem.copyFromLocalFile(true, true, new Path(userDir + "/target/mumu-spark.jar"), new Path(hadoopAddress() + "/mumu/spark/jar/"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } finally {
            try {
                distributedFileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
