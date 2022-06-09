package com.test.app.dwd.db;

/**
 * 交易域退单事务事实表
 * 主要任务:
 * 从 Kafka 读取业务数据，筛选退单表数据，筛选满足条件的订单表数据，建立 MySQL-Lookup 字典表，关联三张表获得退单明细宽表。
 *思路分析:
 * 1）筛选退单表数据
 * 	退单业务过程最细粒度的操作为一个订单中一个 SKU 的退单操作，退单表粒度与最细粒度相同，将其作为主表。
 * 2）筛选订单表数据并转化为流
 * 	获取 province_id。退单操作发生时，订单表的 order_status 字段值会由1002（已支付）更新为 1005（退款中）。订单表中的数据要满足三个条件：
 * （1）order_status 为 1005（退款中）；
 * （2）操作类型为 update；
 * （3）更新的字段为 order_status。
 * 该字段发生变化时，变更数据中 old 字段下 order_status 的值不为 null（为 1002）。
 * 3）建立 MySQL-Lookup 字典表
 * 	获取退款类型名称和退款原因类型名称。
 * 4）关联这几张表获得退单明细宽表，写入 Kafka 退单明细主题
 * 	主表中的数据都与退单业务相关，因此所有关联均用左外联即可。第二步是否对订单表数据筛选并不影响查询结果，提前对数据进行过滤是为了减少数据量，减少性能消耗。
 *
 */
public class DwdTradeOrderRefund {
    public static void main(String[] args) {

        //TODO 1. 环境准备
        
    }
}
