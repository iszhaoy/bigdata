package table.source.redis;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.Util;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.container.RedisContainer;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;


/**
 * Bahir Flink项目已经提供了基于DataStream API的RedisSink，我们可以利用它来直接构建RedisDynamicTableSink，
 * 减少重复工作。实现了DynamicTableSink接口的类骨架如下.
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    private final ReadableConfig options;
    private final TableSchema schema;

    public RedisDynamicTableSink(ReadableConfig options, TableSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    /**
     * getChangelogMode()方法需要返回该Sink可以接受的change log行的类别。
     * 由于向Redis写入的数据可以是只追加的，也可以是带有回撤语义的（如各种聚合数据），
     * 因此支持INSERT、UPDATE_BEFORE和UPDATE_AFTER类别。
     *
     * @param changelogMode
     * @return
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Preconditions.checkNotNull(options, "No options supplied");

        String mode = options.get(RedisDynamicTableFactory.MODE);
        String host = options.get(RedisDynamicTableFactory.SINGLE_HOST);
        int port = options.get(RedisDynamicTableFactory.SINGLE_PORT);
        FlinkJedisConfigBase jedisConfig = null;
        if ("single".equals(mode)) {
            jedisConfig = new FlinkJedisPoolConfig.Builder().setHost(host).build();
        }
        //else if ("cluster".equals(mode)) {
        //    jedisConfig = new FlinkJedisClusterConfig.Builder().setNodes();
        //}
        Preconditions.checkNotNull(jedisConfig, "No Jedis config supplied");

        RedisCommand command = RedisCommand.valueOf(options.get(RedisDynamicTableFactory.COMMAND).toUpperCase());

        int fieldCount = schema.getFieldCount();
        if (fieldCount != (needAdditionalKey(command) ? 3 : 2)) {
            throw new ValidationException("Redis sink only supports 2 or 3 columns");
        }

        DataType[] dataTypes = schema.getFieldDataTypes();
        for (int i = 0; i < fieldCount; i++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                throw new ValidationException("Redis connector only supports STRING type");
            }
        }

        RedisMapper<RowData> mapper = new RedisRowDataMapper(options, command);
        RiskSinkMyself<RowData> redisSink = new RiskSinkMyself<RowData>(jedisConfig, mapper);
        return SinkFunctionProvider.of(redisSink);
    }

    private static boolean needAdditionalKey(RedisCommand command) {
        return command.getRedisDataType() == RedisDataType.HASH || command.getRedisDataType() == RedisDataType.SORTED_SET;
    }

    @Override
    public DynamicTableSink copy() {
         return new RedisDynamicTableSink(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Sink";
    }

    public static final class RedisRowDataMapper implements RedisMapper<RowData> {
        private static final long serialVersionUID = 1L;

        private final ReadableConfig options;
        private final RedisCommand command;

        public RedisRowDataMapper(ReadableConfig options, RedisCommand command) {
            this.options = options;
            this.command = command;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(command, "default-additional-key");
        }

        @Override
        public String getKeyFromData(RowData data) {
            return data.getString(needAdditionalKey(command) ? 1 : 0).toString();
        }

        @Override
        public String getValueFromData(RowData data) {
            return data.getString(needAdditionalKey(command) ? 2 : 1).toString();
        }

        @Override
        public Optional<String> getAdditionalKey(RowData data) {
            return needAdditionalKey(command) ? Optional.of(data.getString(0).toString()) : Optional.empty();
        }

        @Override
        public Optional<Integer> getAdditionalTTL(RowData data) {
            return options.getOptional(RedisDynamicTableFactory.TTL_SEC);
        }
    }

    static class RiskSinkMyself<IN> extends RichSinkFunction<IN> {

        private static final long serialVersionUID = 1L;

        private static final Logger LOG = LoggerFactory.getLogger(RiskSinkMyself.class);

        private String additionalKey;

        private Integer additionalTTL;

        private RedisMapper<IN> redisSinkMapper;
        private RedisCommand redisCommand;

        private FlinkJedisConfigBase flinkJedisConfigBase;
        private RedisCommandsContainer redisCommandsContainer;

        /**
         * Creates a new {@link RiskSinkMyself} that connects to the Redis server.
         *
         * @param flinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
         * @param redisSinkMapper This is used to generate Redis command and key value from incoming elements.
         */
        public RiskSinkMyself(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper) {
            Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
            Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
            Objects.requireNonNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");

            this.flinkJedisConfigBase = flinkJedisConfigBase;

            this.redisSinkMapper = redisSinkMapper;
            RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();

            this.redisCommand = redisCommandDescription.getCommand();
            this.additionalTTL = redisCommandDescription.getAdditionalTTL();
            this.additionalKey = redisCommandDescription.getAdditionalKey();
        }

        /**
         * Called when new data arrives to the sink, and forwards it to Redis channel.
         * Depending on the specified Redis data type (see {@link RedisDataType}),
         * a different Redis command will be applied.
         * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
         *
         * @param input The incoming data
         */
        @Override
        public void invoke(IN input, Context context) throws Exception {
            String key = redisSinkMapper.getKeyFromData(input);
            String value = redisSinkMapper.getValueFromData(input);

            Optional<String> optAdditionalKey = redisSinkMapper.getAdditionalKey(input);
            Optional<Integer> optAdditionalTTL = redisSinkMapper.getAdditionalTTL(input);

            switch (redisCommand) {
                case RPUSH:
                    this.redisCommandsContainer.rpush(key, value);
                    break;
                case LPUSH:
                    this.redisCommandsContainer.lpush(key, value);
                    break;
                case SADD:
                    this.redisCommandsContainer.sadd(key, value);
                    break;
                case SET:
                    this.redisCommandsContainer.set(key, value);
                    break;
                case SETEX:
                    this.redisCommandsContainer.setex(key, value, optAdditionalTTL.orElse(this.additionalTTL));
                    break;
                case PFADD:
                    this.redisCommandsContainer.pfadd(key, value);
                    break;
                case PUBLISH:
                    this.redisCommandsContainer.publish(key, value);
                    break;
                case ZADD:
                    this.redisCommandsContainer.zadd(optAdditionalKey.orElse(this.additionalKey), value, key);
                    break;
                case ZINCRBY:
                    this.redisCommandsContainer.zincrBy(optAdditionalKey.orElse(this.additionalKey), value, key);
                    break;
                case ZREM:
                    this.redisCommandsContainer.zrem(optAdditionalKey.orElse(this.additionalKey), key);
                    break;
                case HSET:
                    this.redisCommandsContainer.hset(optAdditionalKey.orElse(this.additionalKey), key, value,
                            optAdditionalTTL.orElse(this.additionalTTL));
                    break;
                case HINCRBY:
                    this.redisCommandsContainer.hincrBy(optAdditionalKey.orElse(this.additionalKey), key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                    break;
                case INCRBY:
                    this.redisCommandsContainer.incrBy(key, Long.valueOf(value));
                    break;
                case INCRBY_EX:
                    this.redisCommandsContainer.incrByEx(key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                    break;
                case DECRBY:
                    this.redisCommandsContainer.decrBy(key, Long.valueOf(value));
                    break;
                case DESCRBY_EX:
                    this.redisCommandsContainer.decrByEx(key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                    break;
                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
            }
        }

        /**
         * Initializes the connection to Redis by either cluster or sentinels or single server.
         *
         * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            try {
                this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
                this.redisCommandsContainer.open();
            } catch (Exception e) {
                LOG.error("Redis has not been properly initialized: ", e);
                throw e;
            }
        }

        /**
         * Closes commands container.
         * @throws IOException if command container is unable to close.
         */
        @Override
        public void close() throws IOException {
            if (redisCommandsContainer != null) {
                redisCommandsContainer.close();
            }
        }
    }
}



