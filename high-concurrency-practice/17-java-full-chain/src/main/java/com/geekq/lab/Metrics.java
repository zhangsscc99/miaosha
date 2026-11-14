package com.geekq.lab;

import java.util.concurrent.atomic.LongAdder;

/** 服务端计数器，/stats 暴露，用于核对压测客户端与真实处理量是否一致。 */
public class Metrics {
    public final LongAdder requests = new LongAdder();
    public final LongAdder ok = new LongAdder();
    public final LongAdder accepted = new LongAdder();
    public final LongAdder soldOut = new LongAdder();
    public final LongAdder limited = new LongAdder();
    public final LongAdder errors = new LongAdder();
    public final LongAdder backpressure = new LongAdder();

    public void reset() {
        requests.reset();
        ok.reset();
        accepted.reset();
        soldOut.reset();
        limited.reset();
        errors.reset();
        backpressure.reset();
    }

    public String toJson() {
        return "{\"requests\":" + requests.sum()
                + ",\"ok\":" + ok.sum()
                + ",\"accepted\":" + accepted.sum()
                + ",\"soldOut\":" + soldOut.sum()
                + ",\"limited\":" + limited.sum()
                + ",\"errors\":" + errors.sum()
                + ",\"backpressure\":" + backpressure.sum() + "}";
    }
}
