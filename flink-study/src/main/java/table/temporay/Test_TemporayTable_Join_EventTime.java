package table.temporay;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class Test_TemporayTable_Join_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        TableConfig config = tEnv.getConfig();
        config.setIdleStateRetention(Duration.ofHours(24));
        // append-only
        String createOrdersDDL = "\n"
                + " create table orders (\n"
                + " id string,\n"
                + " currency string,\n"
                + " ts string,\n"
                + " proctime as proctime(),\n"
                + " rowtime as coalesce(to_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),0),\n"
                + " watermark for rowtime as rowtime - interval '0' second\n"
                + ") with (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'Orders',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'properties.bootstrap.servers' = 'kafka:9092',\n"
                + "  'format' = 'csv',\n"
                + "  'csv.ignore-parse-errors' = 'true'\n"
                + ")";
        System.out.println(createOrdersDDL);

        // append-only
        String createRatesDDL = "\n"
                + " create table rates (\n"
                + " currency string,\n"
                + " rate string,\n"
                + " ts string,\n"
                + " proctime as proctime(),\n"
                + " rowtime as coalesce(to_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),0),\n"
                + " watermark for rowtime as rowtime - interval '0' second\n"
                + ") with (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'Rates',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'properties.bootstrap.servers' = 'kafka:9092',\n"
                + "  'format' = 'csv',\n"
                + "  'csv.ignore-parse-errors' = 'true'\n"
                + ")";

        // 将版本视图将rates append-only 转换为 changelog
        String transformDML = "create view chageLogRates as select\n" +
                "    currency,\n" +
                "    rate,\n" +
                "    rowtime\n" +
                "from (\n" +
                "    select \n" +
                "        currency,\n" +
                "        rate,\n" +
                "        rowtime,\n" +
                "        row_number() over(partition by currency order by rowtime desc) as rk \n" +
                "    from rates \n" +
                ") where rk = 1";

        tEnv.executeSql(createOrdersDDL);
        tEnv.executeSql(createRatesDDL);
        tEnv.executeSql(transformDML);

        String query1 = "select id,currency,rowtime from orders";
        String query2 = "select currency,rate,rowtime from chageLogRates";
        String query3 = "select o.id,o.currency,r.rate from orders o left join chageLogRates for system_time as of o.rowtime as r on o.currency = r.currency";

        tEnv.toAppendStream(tEnv.sqlQuery(query1), Row.class).print("orders");
        tEnv.toRetractStream(tEnv.sqlQuery(query2), Row.class).print("chageLogRates");
        tEnv.toRetractStream(tEnv.sqlQuery(query3), Row.class).print("result");

        env.execute();
    }
}
