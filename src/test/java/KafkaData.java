import cn.hutool.json.JSONUtil;
import com.ques.model.TagData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class KafkaData {
    public static void main(String[] args) {
        // 创建消费者对象
        Properties props = new Properties();
        // 设置kafka服务地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 设置key-value序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建生产对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        // 测点数据
        TagData tagData = new TagData();
        long dataTime = System.currentTimeMillis();
        tagData.setDataTime(dataTime);
        tagData.setTagName("test_tag");
        tagData.setValue(5d);
//        tagData.setValue(Double.valueOf(new Date().getSeconds()));

        // 发送数据到kafka
        producer.send(new ProducerRecord<>("test_source_topic", JSONUtil.toJsonStr(tagData)));
        producer.flush();

        producer.close();
    }
}
