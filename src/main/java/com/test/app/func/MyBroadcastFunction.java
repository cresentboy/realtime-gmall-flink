package com.test.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.test.bean.TableProcess;
import com.test.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * processElement: 1.获取广播的配置数据
 *                  2.过滤字段 filterColumn
 *                  3.补充SinkTable字段输出
 *
 * processBroadcastElement: 1.获取并解析数据，方便操作主流
 *                          2.校验表是否存在，如果不存在则需要在Phoenix中建表checkTable
 *                          3.写入状态，广播出去
 *
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String, TableProcess> tableConfigDescriptor;

    private Connection connection;

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> tableConfigDescriptor) {
        this.tableConfigDescriptor = tableConfigDescriptor;
    }


    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> tableConfigState = readOnlyContext.getBroadcastState(tableConfigDescriptor);

        //获取配置信息
        String sourceTable = jsonObject.getString("table");
        TableProcess tableConfig = tableConfigState.get(sourceTable);
        if(tableConfig != null){
            JSONObject data = jsonObject.getJSONObject("data");
            String sinkTable = tableConfig.getSinkTable();

            //根据sinkColumns 过滤数据
            String sinkColumns = tableConfig.getSinkColumns();
            filterColumns(data,sinkColumns);

            //将目标表名加入到主流数据中
            data.put("sinkTable",sinkTable);
            collector.collect(data);
        }

    }

    /**
     * 校验字段，过滤掉多余的字段
     * @param data
     * @param sinkColumns
     */
    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> dataEntries = data.entrySet();
        dataEntries.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> collector) throws Exception {
        //1.获取并解析数据为JavaBean对象
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        TableProcess config = jsonObject.getObject("after", TableProcess.class);
        String sourceTable = config.getSourceTable();
        String sinkTable = config.getSinkTable();
        String sinkColumns = config.getSinkColumns();
        String sinkPk = config.getSinkPk();
        String sinkExtend = config.getSinkExtend();

        BroadcastState<String, TableProcess> tableConfigState = context.getBroadcastState(tableConfigDescriptor);
        tableConfigState.put(sourceTable,config);
        //2.校验表是否存在,如果不存在则建表
        checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);


    }

    /**
     * Phoenix 建表函数
     *
     * @param sinkTable 目标表名  eg. test
     * @param sinkColumns 目标表字段  eg. id,name,sex
     * @param sinkPk 目标表主键  eg. id
     * @param sinkExtend 目标表建表扩展字段  eg. ""
     *                   eg. create table if not exists mydb.test(id varchar primary key, name varchar, sex varchar)...
     */

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 封装建表 SQL
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists" + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "\n");
        String[] columnArr = sinkColumns.split(",");

        // 为主键及扩展字段赋默认值
        if (sinkPk == null){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }

        //遍历添加字段信息
        for (int i = 0; i < columnArr.length; i++){
            sql.append(columnArr[i] + "varchar");

            //判断当前字段是否为逐渐
            if(sinkPk.equals(columnArr[i])){
                sql.append("primary key");
            }

            //如果当前字段不是最后一个字段，则追加“，”
            if(i < columnArr.length - 1){
                sql.append(",\n");
            }
            sql.append(")");
            sql.append(sinkExtend);
            String createState = sql.toString();

            //为数据库操作对象赋默认值，执行建表SQL
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.prepareStatement(createState);
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("建表语句\n" + createState + "\n执行异常");
            }finally {
                if(preparedStatement != null){
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException("数据库操作对象释放异常");
                    }
                }
            }


        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }




}
