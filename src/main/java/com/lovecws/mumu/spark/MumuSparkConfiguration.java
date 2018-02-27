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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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
    //private static String SPARK_MASTER = "mesos://192.168.11.25:5050";
    //private static String SPARK_MASTER = "yarn";

    private static String HADOOP_URL = "hdfs://192.168.11.25:9000";

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
        if (SPARK_MASTER.contains("yarn") && System.getenv("HADOOP_CONF_DIR") == null && System.getenv("YARN_CONF_DIR") == null) {
            String path = MumuSparkConfiguration.class.getResource("/hadoop").getPath();
            System.setProperty("HADOOP_CONF_DIR", path);
        }
        if (SPARK_MASTER.contains("mesos")) {
            System.setProperty("MESOS_NATIVE_JAVA_LIBRARY", "D:\\program\\mesos-1.5.0\\libmesos-1.5.0.so");
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

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("usage [class mathod args]");
            System.exit(-1);
        }
        String clazz = null, method = null;
        Object[] objects = new Object[]{};
        clazz = args[0];
        method = args[1];
        if (args.length > 2) {
            objects = Arrays.copyOfRange(args, 2, args.length);
        }
        try {
            Class aClass = Class.forName(clazz);
            //对象必须要默认构造方法
            Object newInstance = aClass.newInstance();
            Method[] methods = aClass.getMethods();
            for (Method method1 : methods) {
                if (method1.getName().equalsIgnoreCase(method) && (method1.getParameterCount() == objects.length)) {
                    method1.invoke(newInstance, objects);
                    break;
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
    }
}
