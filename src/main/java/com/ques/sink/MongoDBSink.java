package com.ques.sink;

import cn.hutool.json.JSONUtil;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ques.model.AlarmData;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.Map;

public class MongoDBSink extends RichSinkFunction<AlarmData> {

    MongoClient mongoClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取全局配置
        RuntimeContext runtimeContext = this.getRuntimeContext();
        Map<String, String> globalConfig = runtimeContext.getExecutionConfig().getGlobalJobParameters().toMap();
        String mongodbAddress = globalConfig.get("mongodb_address");
        // 创建 MongoClient
        mongoClient = MongoClients.create(mongodbAddress);
    }

    @Override
    public void invoke(AlarmData value, Context context) throws Exception {
        super.invoke(value, context);

        // 实体体对象转换成mongodb文档
        Document parse = Document.parse(JSONUtil.toJsonStr(value));
        // 插入mongodb
        insertOne("alarm_data", value.getTagName(), parse);
    }

    private void insertOne(String mongodbDatabase, String colName, Document doc) {
        MongoDatabase database = mongoClient.getDatabase(mongodbDatabase);
        MongoCollection<Document> collection = database.getCollection(colName);
        collection.insertOne(doc);
    }
}
