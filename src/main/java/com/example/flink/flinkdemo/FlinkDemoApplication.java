package com.example.flink.flinkdemo;

import com.example.flink.flinkdemo.source.DiySource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class FlinkDemoApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(FlinkDemoApplication.class, args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*1. kafka Source*/
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.221:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "192.168.1.221:2181");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), properties));

        /* 2.自定义 Source*/
        DataStream<Double> stream2 = env.addSource(new DiySource());

        /**
         * 根据hashCode分组
         */
        DataStream<Double> stream3 = stream2.keyBy(x -> x).sum(0);
        DataStream<Double> stream4 = stream2.keyBy(x -> x).reduce(Double::sum);
        /**
         * splite分流及select选择流(已过时)
         */
        SplitStream<Double> stream5 = stream2.split((OutputSelector<Double>) value -> {
            List<String> output = new ArrayList<>();
            if (value instanceof Double) {
                output.add("double");
            } else {
                output.add("notDouble");
            }
            return output;
        });
        DataStream<Double> isDouble = stream5.select("double");
        DataStream<Double> notDouble = stream5.select("notDouble");

        /**
         * collect + map合并流
         */
        ConnectedStreams<Double, Double> collectStream = isDouble.connect(notDouble);
        DataStream<Double> mapStream = collectStream.map(new CoMapFunction<Double, Double, Double>() {
            @Override
            public Double map1(Double value) throws Exception {
                return value;
            }

            @Override
            public Double map2(Double value) throws Exception {
                return value;
            }
        });

        /**
         * union 合并<相同类型>的流，但可以是多条
         */
        DataStream<Double> unionStream = isDouble.union(notDouble);


        stream3.print().setParallelism(1);
        env.execute("test");
    }

}
