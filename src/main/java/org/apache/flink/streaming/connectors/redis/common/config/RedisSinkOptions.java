package org.apache.flink.streaming.connectors.redis.common.config;

/** sink options. @Author: Jeff Zou @Date: 2022/9/28 16:36 */
public class RedisSinkOptions {
    private final int maxRetryTimes;

    private final RedisValueDataStructure redisValueDataStructure;

    public String getKeyPrefix() {
        return keyPrefix;
    }

    private  String keyPrefix;

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public RedisValueDataStructure getRedisValueDataStructure() {
        return redisValueDataStructure;
    }

    public RedisSinkOptions(int maxRetryTimes, RedisValueDataStructure redisValueDataStructure,String keyPrefix) {
        this.maxRetryTimes = maxRetryTimes;
        this.redisValueDataStructure = redisValueDataStructure;
        this.keyPrefix=keyPrefix;
    }

    /** RedisSinkOptions.Builder. */
    public static class Builder {
        private int maxRetryTimes;
        private String keyPrefix;
        private RedisValueDataStructure redisValueDataStructure;

        public Builder setRedisValueDataStructure(RedisValueDataStructure redisValueDataStructure) {
            this.redisValueDataStructure = redisValueDataStructure;
            return this;
        }

        public Builder setKeyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
            return this;
        }

        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }
        public RedisSinkOptions build() {
            return new RedisSinkOptions(maxRetryTimes, redisValueDataStructure,keyPrefix);
        }
    }
}
