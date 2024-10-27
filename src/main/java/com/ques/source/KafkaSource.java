package com.ques.source;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.ques.model.TagData;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * 定义一个输出数据类型为TagData格式的数据源
 */
public class KafkaSource extends RichSourceFunction<TagData> {

    KafkaConsumer<String, String> consumer;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 获取全局配置
        RuntimeContext runtimeContext = this.getRuntimeContext();
        Map<String, String> globalConfig = runtimeContext.getExecutionConfig().getGlobalJobParameters().toMap();
        String kafkaAddress = globalConfig.get("kafka_address");

        // 创建消费者对象
        Properties props = new Properties();
        // 设置kafka服务地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        // 指定消费者组（避免多节点同时消费一个topic的重复消费的问题）
        props.put(ConsumerConfig.GROUP_ID_CONFIG, globalConfig.get("kafka_group_id"));
        // 开启自动提交配置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交时间间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        // 心跳时间检测间隔
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 6000);
        // 消费者组失效超时时间
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 28000);
        // 消费者组位移丢失和越界后恢复起始位置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 最大拉取时间间隔
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        // 一次最大拉取的量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

        // 设置key-value反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(globalConfig.get("kafka_source_topic")));
    }

    @Override
    public void run(SourceContext<TagData> sourceContext) throws Exception {
        // 通过死循环持续消费消息
        while (true) {
            try {
                // 拉取数据
                // 这里的1000表示拉取超时时间，单位是毫秒，意思是等待1000毫秒，如果1000毫秒内没有拉取到数据就返回了
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (CollectionUtil.isNotEmpty(records)){
                    for (ConsumerRecord<String, String> record : records) {
                        // 获取kafka数据
                        String data = record.value();
                        // 数据封装
                        TagData tagData = JSONUtil.toBean(data, TagData.class);
                        // 将数据发送到下游
                        sourceContext.collect(tagData);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    @Override
    public void cancel() {
        consumer.close();
    }
}
