package com.example.flink.flinkdemo.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class DiySource implements SourceFunction<Double> {

    Boolean running = true;

    /**
     * 生成数据的方式
     */
    @Override
    public void run(SourceContext<Double> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(new Random().nextGaussian());
            Thread.sleep(500);
        }
    }

    /**
     * 取消数据生成
     */
    @Override
    public void cancel() {
        running = false;
    }
}
