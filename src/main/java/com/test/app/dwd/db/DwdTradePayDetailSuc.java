package com.test.app.dwd.db;

/**
 * 交易域支付成功事务事实表
 * 主要任务:
 * 从 Kafka topic_db主题筛选支付成功数据、从dwd_trade_order_detail主题中读取订单事实数据、MySQL-LookUp字典表，
 * 关联三张表形成支付成功宽表，写入 Kafka 支付成功主题。
 * 思路分析:
 * 1）获取订单明细数据
 * 用户必然要先下单才有可能支付成功，因此支付成功明细数据集必然是订单明细数据集的子集。
 * 2）筛选支付表数据
 * 获取支付类型、回调时间（支付成功时间）、支付成功时间戳。
 * 3）构建 MySQL-LookUp 字典表
 * 4）关联上述三张表形成支付成功宽表，写入 Kafka 支付成功主题
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) {

    }
}
