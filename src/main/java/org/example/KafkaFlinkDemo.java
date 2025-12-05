package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaFlinkDemo {
    public static void main(String[] args) throws Exception {

        // 1. 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Kafka 参数
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group");

        // 3. 构建 Kafka Consumer，从 topic 输入
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("input-topic", new SimpleStringSchema(), props);

        // 4. 添加 Source
        DataStreamSource<String> stream = env.addSource(consumer);

        // 5. 数据处理：加一个前缀
        stream
                .map(value -> "Processed: " + value)
                .print();

        // 6. 写出到 Kafka
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<>(
                        "output-topic",
                        new SimpleStringSchema(),
                        props
                );

        stream.map(value -> "OUT: " + value)
                .addSink(producer);

        // 7. 启动
        env.execute("Flink Kafka Demo");
    }
}
