package com.test.app.func;

import com.alibaba.fastjson.JSONObject;
import com.test.utils.DimUtil;
import com.test.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.concurrent.ExecutorService;

/**
 * 异步 IO 函数 DimAsyncFunction
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    private String tableName;

    private ExecutorService executorService;

    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService  =ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {

        //从线程池中获取线程，发送异步请求
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try{
                            long start = System.currentTimeMillis();
                            //1.根据流中的对象获取维度的主键
                            String key = getKey(obj);
                            //2.根据维度的主键获取维度对象
                            JSONObject dimJsonObj = DimUtil.getDimInfo(tableName, key);
                            //3. 将查询出来的维度信息 补充流中的对象属性
                            if(dimJsonObj != null){
                                join(obj,dimJsonObj);
                            }
                            long end = System.currentTimeMillis();
                            System.out.println("异步维度查询共耗时:" + (end - start) + "毫秒");
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("异步维度关联发生异常了");
                        }
                    }
                }
        );
    }
}
