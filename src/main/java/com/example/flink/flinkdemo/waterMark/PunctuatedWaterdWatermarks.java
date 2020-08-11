package com.example.flink.flinkdemo.waterMark;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class PunctuatedWaterdWatermarks implements AssignerWithPunctuatedWatermarks<Double> {

    long bound = 60 * 1000;

    /**
     * 检查是否符合某些条件，符合才生产waterMark
     */
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Double lastElement, long extractedTimestamp) {
        if (0 == lastElement) {
            //如果是0不生产watermark
            return null;
        } else {
            return new Watermark(extractedTimestamp - bound);
        }
    }

    @Override
    public long extractTimestamp(Double element, long previousElementTimestamp) {
        //实际应从数据里拿事件时间
        return System.currentTimeMillis();
    }
}
