package com.example.flink.flinkdemo.waterMark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class PeriodicWatermarks implements AssignerWithPeriodicWatermarks<Double> {

    //延迟一分钟
    long bound = 60 * 1000;
    //观察到的最大时间戳
    long maxTs = Long.MIN_VALUE;

    /**
     * 生成waterMark，默认值200ms调用一次
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        /*
         * 水位线就是目前观察到的最大时间-延迟的时间
         * 比如：如果收到的最大事件时间是9点，那么表示8点59之前的数据已经全部收到了
         */
        return new Watermark(maxTs - bound);
    }

    /**
     * 抽取时间戳
     */
    @Override
    public long extractTimestamp(Double element, long previousElementTimestamp) {
        //这个时间戳应该从数据中取
        long elementEventTime = System.currentTimeMillis();
        maxTs = Math.max(maxTs, elementEventTime);
        return elementEventTime;
    }
}
