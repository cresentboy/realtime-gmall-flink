package com.test.bean;

import lombok.Data;

@Data
public class CouponUserOrderBean {
    /**
     * 优惠券领用记录 id
     */
    String id;

    /**
     * 优惠券 id
     */
    String coupon_id;

    /**
     * 用户id
     */
    String user_id;

    /**
     * 订单id
     */
    String order_id;

    /**
     * 优惠券使用日期（下单）
     */
    String date_id;

    /**
     * 优惠券使用时间（下单
     */
    String using_time;

    /**
     * 历史数据
     */
    String old;

    /**
     * 时间戳
     */
    String ts;

}
