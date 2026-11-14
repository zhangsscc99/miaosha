package com.geekq.lab;

/** 一行待落库的订单，仅保留压测需要的最小字段。 */
public record OrderRow(String orderNo, long userId, long goodsId) {
}
