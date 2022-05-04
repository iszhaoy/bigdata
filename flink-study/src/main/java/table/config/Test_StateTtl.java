package table.config;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class Test_StateTtl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 设置状态过期时间
        TableConfig config = tEnv.getConfig();

        // 指定空闲状态(即未更新的状态)将被保留多久的最小和最大时间间隔。除非状态空闲时间小于最小值，否则不会被清除;如果状态空闲时间大于最大值，则不会被保留
        //config.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));
        config.setIdleStateRetention(Duration.ofMillis(10));

        String createKafkaDDL = "\n"
                + " create table orders (\n"
                + " id string,\n"
                + " currency string,\n"
                + " ts timestamp(3),\n"
                + " proctime as proctime(),\n"
                + " rowtime as ts,\n"
                + " watermark for rowtime as rowtime - interval '0' second\n"
                + ") with (\n"
                + "  'connector' = 'filesystem',\n"
                + "  'path' = 'file:///opt/workspace/javaproject/bigdata/flink-study/src/main/resources/data/orders.csv',\n"
                + "  'format' = 'csv',\n"
                + "  'csv.ignore-parse-errors' = 'true'\n"
                + ")";

        tEnv.executeSql(createKafkaDDL);

        String aggDML = "" +
                "select currency,count(*) as cnt " +
                "from orders " +
                "group by currency";

        tEnv.toRetractStream(tEnv.sqlQuery(aggDML), Row.class)
                //.filter(e -> e.f0)
                .print("sink");

        env.execute("Test_StateTtl");

    }
}
