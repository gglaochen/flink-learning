package com.example.flink.flinkdemo;

import com.example.flink.flinkdemo.source.DiySource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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

        stream2.print().setParallelism(1);
        env.execute("test");
    }

}
