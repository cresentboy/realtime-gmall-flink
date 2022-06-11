package com.test.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 用于DwsTradeOrderWindow
 */
@Data
@Builder
@AllArgsConstructor
public class TradeOrderBean {

    /**
     * 窗口起始时间
     */
    String stt;

    /**
     * 窗口关闭时间
     */
    String edt;

    /**
     * 下单独立用户
     *
     */
    Long orderUniqueUserCount;

    /**
     * 下单新用户
     */
    Long orderNewUserCount;

    /**
     * 下单活动减免金额
     */
    Double orderActivityReduceAmount;

    /**
     * 下单优惠券减免金额
     */
    Double orderCouponReduceAmount;

    /**
     * 下单原始金额
     */
    Double orderOriginTotalAmount;

    /**
     *
     */
    Long ts;
}
