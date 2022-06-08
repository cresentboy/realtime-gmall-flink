package com.test.utils;

import com.alibaba.fastjson.JSONObject;
import com.test.common.GmallConfig;
import org.apache.commons.lang.StringUtils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    /**
     * 定义数据库连接对象
     */
    private static Connection connection;

    /**
     * 初始化SQL执行环境
     */
    public static void initializeConnetction(){
        try {
            //1.注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //2.获取连接对象
            connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181");
            //3.设置PhoenixSQL执行使用的schema（对应MySQL的database）
            connection.setSchema(GmallConfig.HBASE_SCHEMA);

        } catch (ClassNotFoundException e) {
            System.out.println("注册驱动异常");
            e.printStackTrace();
        }catch (SQLException e){
            System.out.println("获取连接对象异常");
            e.printStackTrace();
        }
    }

    /**
     * Phoenix 表数据导入方法
     * @param sinkTable 写入数据的 Phoenix 目标表名
     * @param data 待写入的数据
     */
public static void insertValues(String sinkTable, JSONObject data){
    //双重校验锁初始化连接对象
    if(connection == null){
        synchronized (PhoenixUtil.class){
            if (connection == null){
                initializeConnetction();
            }
        }
    }

    //获取字段名
    Set<String> columns = data.keySet();

    //获取字段对应的值
    Collection<Object> values = data.values();

    // 拼接字段名
    String columnStr = StringUtils.join(columns, ",");
    // 拼接字段值
    String valueStr = StringUtils.join(values, "','");

    //拼接插入语句
    String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + columnStr +
            ") values ('" + valueStr + "')";

    //为数据库操作对象赋默认值
    PreparedStatement preparedStatement = null;

    //执行SQL
    try {
        System.out.println("插入语句为：" + sql);
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
        connection.commit();
    }catch (SQLException sqlException){
        sqlException.printStackTrace();
        throw new RuntimeException("数据库操作对象获取或执行异常");
    }finally {
        if (preparedStatement !=null){
            try{
                preparedStatement.close();
            }catch (SQLException sqlException){
                sqlException.printStackTrace();
                throw new RuntimeException("数据库操作对象释放异常");
            }
        }
    }



}
}
