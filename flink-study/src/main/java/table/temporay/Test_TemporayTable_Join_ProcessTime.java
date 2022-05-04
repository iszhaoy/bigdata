package table.temporay;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.function.EavlFormatDateToLong;

import java.util.LinkedList;
import java.util.List;

public class Test_TemporayTable_Join_ProcessTime {
    public static final Logger log = LoggerFactory.getLogger(Test_TemporayTable_Join_ProcessTime.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建StreamTableEnv
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建日期处理函数
        tEnv.createFunction("parseDate", EavlFormatDateToLong.class);
        log.info("注册日期处理函数");

        String createKafkaDDL = "\n"
                + " create table orders (\n"
                + " id string,\n"
                + " currency string,\n"
                + " ts string,\n"
                + " proctime as proctime(),\n"
                //+ " rowtime as to_timestamp(FROM_UNIXTIME(parseDate(ts),'yyyy-MM-dd HH:mm:ss')),\n"
                + " rowtime as COALESCE(to_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),0),\n"
                + " watermark for rowtime as rowtime - interval '5' second\n"
                //+ " PRIMARY KEY (id) NOT ENFORCED\n"
                + ") with (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'Orders',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'properties.bootstrap.servers' = 'kafka:9092',\n"
                + "  'format' = 'csv',\n"
                + "  'csv.ignore-parse-errors' = 'true'\n"
                + ")";

        String createMysqlDDL = "\n"
                + "create table rates (\n"
                + "  currency string,\n"
                + "  rate double \n"
                + ") with (\n"
                + "  'connector' = 'jdbc',\n"
                + "  'url' = 'jdbc:mysql://localhost:3306/local?useSSL=false',\n"
                + "  'table-name' = 'rates',\n"
                + "  'username' = 'root',\n"
                + "  'password' = 'root',\n"
                + "  'lookup.cache.max-rows' = '100',\n"
                + "  'lookup.cache.ttl' = '10' "
                + ")";

        List<String> list = new LinkedList<>();

        list.add(createKafkaDDL);
        list.add(createMysqlDDL);

        for (String ddl : list) {
            log.warn("执行sql：" + ddl);
            tEnv.executeSql(ddl);
        }

        Table ordersTable = tEnv.sqlQuery("select * from orders");
        Table ratesTable = tEnv.sqlQuery("select * from rates");

        // 打印Orders
        tEnv.toAppendStream(ordersTable, Row.class).print();

        // 打印Rates
        tEnv.toAppendStream(ratesTable, Row.class).print();

        String joinDML = "select o.id, o.currency,o.rowtime, r.rate\n" +
                "from \n" +
                "  orders as o left join rates for system_time as of o.proctime as r \n" +
                "on o.currency = r.currency";

        Table joinTable = tEnv.sqlQuery(joinDML);
        // 打印joinTable
        tEnv.toAppendStream(joinTable, Row.class).print();
        env.execute("TestTemporayTable");
    }
}


