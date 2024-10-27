package com.ques.tool;

import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

import java.util.Map;

public class RedissonTool {
    private static volatile RedissonClient redissonClient;

    public static void init(Map<String, String> globalConfig) {
        if (redissonClient == null) {
            Class var1 = RedissonTool.class;
            synchronized(RedissonTool.class) {
                if (redissonClient == null) {
                    try {
                        String redisAddress = (String)globalConfig.get("redis_address");
                        Config redisConfig = new Config();
                        redisConfig.setCodec(new JsonJacksonCodec());
                        redisConfig.useSingleServer()
                                .setAddress(redisAddress)
                                .setPassword(null);
                        redissonClient = Redisson.create(redisConfig);
                        System.out.println("Redis工具类初始化完成！");
                    } catch (Exception var7) {
                        System.out.println("Redis工具类初始化失败：");
                        throw new RuntimeException("Redis工具类初始化失败!", var7);
                    }
                }
            }
        }
    }

    /**
     * 根据测点名，从redis获取规则
     * @param tagName
     * @return
     */
    public static String getRule(String tagName) {
        RBucket<String> bucket = redissonClient.getBucket("rule:" + tagName);
        return bucket.get();
    }
}
