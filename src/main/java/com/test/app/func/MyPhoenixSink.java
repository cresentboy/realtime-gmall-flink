package com.test.app.func;

import com.alibaba.fastjson.JSONObject;
import com.test.utils.DimUtil;
import com.test.utils.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MyPhoenixSink implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        //获取目标表名
        String sinkTable = jsonObject.getString("sinkTable");
        //二次添加
        String type = jsonObject.getString("type");
        String id = jsonObject.getString("id");

        //清楚JSON对象中的sinkTable字段，以便可将该对象直接用于 HBase 表的数据写入
        jsonObject.remove("sinkTable");
        //二次添加
        jsonObject.remove("type");
        PhoenixUtil.insertValues(sinkTable,jsonObject);

        //如果此操作类型为Update，则清楚redis中的缓存信息
        if("update".equals(type)){
            DimUtil.deleteCached(sinkTable,id);
        }
    }
}
