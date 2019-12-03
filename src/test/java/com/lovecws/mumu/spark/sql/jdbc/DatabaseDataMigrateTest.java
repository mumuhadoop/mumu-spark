package com.lovecws.mumu.spark.sql.jdbc;

import org.junit.Test;

/**
 * @program: mumu-spark
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-10-18 15:09
 **/
public class DatabaseDataMigrateTest {

    @Test
    public void pg2gbase() {
        DatabaseDataMigrate databaseDataMigrate = new DatabaseDataMigrate();
        //databaseDataMigrate.pg2gbase("tb_syslog_corp_dict");
        //databaseDataMigrate.pg2gbase("tb_cnvd");
        databaseDataMigrate.pg2gbase("tb_loophole");
        //databaseDataMigrate.pg2gbase("tb_protocol_info");
        //databaseDataMigrate.pg2gbase("tb_protocol_device");
        //databaseDataMigrate.pg2gbase("tb_protocol_service");
//        databaseDataMigrate.pg2gbase("tb_protocol_vendor");
    }
}
