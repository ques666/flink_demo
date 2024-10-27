import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

public class RedisRule {
    public static void main(String[] args) {
        Config redisConfig = new Config();
        redisConfig.setCodec(new JsonJacksonCodec());
        redisConfig.useSingleServer()
                .setAddress("redis://127.0.0.1:6379");
        RedissonClient redissonClient = Redisson.create(redisConfig);

        // 设置test_tag的规则是
        redissonClient.getBucket("rule:test_tag").set("10>x&&x>0");

        redissonClient.shutdown();
    }
}
