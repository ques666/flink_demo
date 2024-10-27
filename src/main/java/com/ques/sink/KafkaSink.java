package com.ques.sink;

import cn.hutool.json.JSONUtil;
import com.ques.model.CalcResult;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class KafkaSink extends RichSinkFunction<CalcResult> {

    KafkaProducer<String, String> producer;

    String kafkaTopic;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取全局配置
        RuntimeContext runtimeContext = this.getRuntimeContext();
        Map<String, String> globalConfig = runtimeContext.getExecutionConfig().getGlobalJobParameters().toMap();
        String kafkaAddress = globalConfig.get("kafka_address");
        kafkaTopic = globalConfig.get("kafka_sink_topic");

        // 创建消费者对象
        Properties props = new Properties();
        // 设置kafka服务地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        // 设置key-value序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建生产对象
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void invoke(CalcResult value, Context context) throws Exception {
        // 发送消息
        producer.send(new ProducerRecord<>(kafkaTopic, JSONUtil.toJsonStr(value)));
        producer.flush();
    }

    @Override
    public void close() throws Exception {
        super.close();
        producer.close();
    }
}
