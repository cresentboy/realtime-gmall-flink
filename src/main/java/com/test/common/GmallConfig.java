package com.test.common;

/**
 * 配置常量类
 *
 */
public class GmallConfig {
    /**
     * phoenix库名
     */
    public static final String HBASE_SCHEMA = "GMALL2022_REALTIME";

    /**
     * phoenix驱动
     */
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    /**
     * phoenix连接参数
     */
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
