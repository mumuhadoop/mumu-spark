package com.lovecws.mumu.spark.streaming;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 文件spark streaming 只监听新的文件，对比文件的修改时间和当前时间，如果是当前时间之前创建的文件 则不会监听文件
 * TODO 注意点:
 * 1 所有文件必须具有相同的数据格式
 * 2 所有文件必须在`dataDirectory`目录下创建， 文件是自动的移动和重命名到数据目录下
 * 3 一旦移动， 文件必须被修改。 所以如果文件被持续的附加数据， 新的数据不会被读取。
 * @date 2017-10-30 16:36
 */
public class FileSparkStreaming implements Serializable {

    private static final Logger log = Logger.getLogger(FileSparkStreaming.class);

    /**
     * 使用spark streaming监听文件目录下的新文件添加
     *
     * @param textFile      监听文件目录
     * @param batchDuration 监听间隔
     */
    public void fileStreaming(String textFile, long batchDuration) {
        JavaStreamingContext streamingContext = new MumuSparkConfiguration().javaStreamingContext(batchDuration);
        JavaDStream lines = streamingContext.textFileStream(textFile);
        lines.print();
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Object, Iterator>() {
            @Override
            public Iterator call(final Object line) throws Exception {
                return Arrays.asList(line.toString().split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordscount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordscount.print();
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
            streamingContext.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}