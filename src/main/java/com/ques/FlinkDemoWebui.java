package com.ques;

import com.ques.model.AlarmData;
import com.ques.model.CalcResult;
import com.ques.model.TagData;
import com.ques.process.RuleCalc;
import com.ques.sink.KafkaSink;
import com.ques.sink.MongoDBSink;
import com.ques.source.KafkaSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class FlinkDemoWebui {

    /**
     * 报警快照旁路输出标签
     */
    private static  OutputTag<AlarmData> alarmDataOutputTag = new OutputTag<AlarmData>("alarmData"){};

    public static void main(String[] args) throws Exception {
        // 创建webui运行环境
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

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
        DataStream<TagData> dataStream = env.addSource(new KafkaSource())
                .name("kafka数据源");  // 设置算子名称

        // 计算规则算子
        SingleOutputStreamOperator<CalcResult> process = dataStream.process(new RuleCalc())
                .setParallelism(4)     // 设置算子并行度
                .name("规则计算算子");

        // 将计算结果输出到kafka
        process.addSink(new KafkaSink())
                .setParallelism(2)
                .name("输出结果到kafka");

        // 获取旁路报警快照输出
        DataStream<AlarmData> alarmDataOutput = process.getSideOutput(alarmDataOutputTag);
        alarmDataOutput.addSink(new MongoDBSink())
                .setParallelism(2)
                .name("旁路输出报警快照到mongodb");

        // 执行任务
        env.execute("flink程序");
    }
}
