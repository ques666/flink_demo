package com.ques;

import com.ques.model.AlarmData;
import com.ques.model.CalcResult;
import com.ques.model.TagData;
import com.ques.process.RuleCalc;
import com.ques.sink.KafkaSink;
import com.ques.sink.MongoDBSink;
import com.ques.source.KafkaSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class FlinkDemo {

    /**
     * 报警快照旁路输出标签
     */
    private static  OutputTag<AlarmData> alarmDataOutputTag = new OutputTag<AlarmData>("alarmData"){};

    public static void main(String[] args) throws Exception {
        // 获取当前执行环境, 根据当前程序的部署方式不同，来获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局环境
        Map<String, String> configs = new HashMap<>();
        configs.put("kafka_address","127.0.0.1:9092");
        configs.put("kafka_source_topic","test_source_topic");
        configs.put("kafka_group_id","test");
        configs.put("kafka_sink_topic","test_sink_topic");
        configs.put("redis_address","redis://127.0.0.1:6379");
        configs.put("mongodb_address","mongodb://admin:luculent1!@localhost:27017");
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(configs));

        // 接入并拿到kafka数据流
        DataStream<TagData> dataStream = env.addSource(new KafkaSource());

        // 计算规则算子
        SingleOutputStreamOperator<CalcResult> process = dataStream.process(new RuleCalc());

        // 将计算结果输出到kafka
        process.addSink(new KafkaSink());

        // 获取旁路报警快照输出
        DataStream<AlarmData> alarmDataOutput = process.getSideOutput(alarmDataOutputTag);
        alarmDataOutput.addSink(new MongoDBSink());

        // 执行任务
        env.execute("flink程序");
    }
}
