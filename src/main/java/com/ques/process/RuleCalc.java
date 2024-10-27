package com.ques.process;

import com.googlecode.aviator.AviatorEvaluator;
import com.ques.model.AlarmData;
import com.ques.model.CalcResult;
import com.ques.model.TagData;
import com.ques.tool.RedissonTool;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * 创建一个数据流算子，根据传进来的数据，计算规则是否触发
 */
public class RuleCalc extends ProcessFunction<TagData, CalcResult> {

    /**
     * 报警快照旁路输出标签
     */
    OutputTag<AlarmData> alarmDataOutputTag = new OutputTag<AlarmData>("alarmData"){};

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取全局配置
        RuntimeContext runtimeContext = this.getRuntimeContext();
        Map<String, String> globalConfig = runtimeContext.getExecutionConfig().getGlobalJobParameters().toMap();
        // 初始化Redisson
        RedissonTool.init(globalConfig);
    }

    @Override
    public void processElement(TagData tagData, ProcessFunction<TagData, CalcResult>.Context context, Collector<CalcResult> collector) throws Exception {
        // 避免因为一个数据出错，导致整个程序异常，使用try...catch来捕获一下异常
        try {
            // 根据tagData的测点从redis中读取规则，规则例如：10>x&&x>0
            String tagName = tagData.getTagName();
            String rule = RedissonTool.getRule(tagName);
            // 使用Aviator将测点值带入规则进行计算
            Map<String,Object> env = new HashMap<>();
            env.put("x", tagData.getValue());
            Boolean execute = (Boolean) AviatorEvaluator.execute(rule, env);
            // 封装返回结果
            CalcResult result = new CalcResult();
            result.setTagName(tagName);
            result.setAlarm(execute);
            result.setDataTime(tagData.getDataTime());
            // 将计算结果返回到下游
            collector.collect(result);

            // 如果报警触发，记录报警快照
            if(execute){
                // 封装报警快照数据
                AlarmData alarmData = new AlarmData();
                alarmData.setTagName(tagName);
                alarmData.setRule(rule);
                alarmData.setValue(tagData.getValue());
                alarmData.setDataTime(tagData.getDataTime());

                // 旁路分支输出报警快照
                // 因为算子的主输出格式单一的，已经有了CalcResult类型的输出格式，要向输出其他格式，需要使用旁路分支的方式输出
                // 具体参考文档：https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/stream/side_output.html
                context.output(alarmDataOutputTag, alarmData);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
