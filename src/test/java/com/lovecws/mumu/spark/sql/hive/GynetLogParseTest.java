package com.lovecws.mumu.spark.sql.hive;

import org.junit.Test;

/**
 * @program: mumu-spark
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-05-05 14:01
 **/
public class GynetLogParseTest {

    @Test
    public void gynetLogParse() {
        new DnsLogParse().emailLogParse("E:\\data\\hive\\min=9");
    }
}
