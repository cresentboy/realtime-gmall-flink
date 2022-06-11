package com.test.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 模板方法设计模式模板接口 DimJoinFunction
 * @param <T>
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj) throws Exception;

    //获取维度主键的方法
    String getKey(T obj);
}
