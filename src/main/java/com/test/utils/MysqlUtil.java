package com.test.utils;

public class MysqlUtil {

    public static String getBaseDickUpDDL(){

        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");

    }

    /**
     * JDBC SQL Connector 参数解读
     * 	connector：连接器类型，此处为 jdbc
     * 	url：数据库 url
     * 	table-name：数据库中表名
     * 	lookup.cache.max-rows：lookup 缓存中的最大记录条数
     * 	lookup.cache.ttl：lookup 缓存中每条记录的最大存活时间
     * 	username：访问数据库的用户名
     * 	password：访问数据库的密码
     * 	driver：数据库驱动，注意：通常注册驱动可以省略，但是自动获取的驱动是 com.mysql.jdbc.Driver，
     * Flink CDC 2.1.0 要求 mysql 驱动版本必须为 8 及以上，在 mysql-connector -8.x 中该驱动已过时，
     * 新的驱动为 com.mysql.cj.jdbc.Driver。
     * @param tableName
     * @return
     */
    private static String mysqlLookUpTableDDL(String tableName) {

        String ddl = "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = 'root',\n" +
                "'password' = '000000',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
        return ddl;

    }
}
