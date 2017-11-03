package com.lovecws.mumu.spark.sql;

import com.lovecws.mumu.spark.sql.SchemaRDDSparkSQL;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: schemaRDD创建测试
 * @date 2017-11-01 13:45
 */
public class SchemaRDDSparkSQLTest {

    private SchemaRDDSparkSQL sparkSQL = new SchemaRDDSparkSQL();

    @Test
    public void reflect() {
        sparkSQL.reflect();
    }

    @Test
    public void struct() {
        sparkSQL.struct();
    }

    @Test
    public void json() {
        sparkSQL.json();
    }
}
