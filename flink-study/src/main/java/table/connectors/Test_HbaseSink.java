package table.connectors;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import table.function.EavlFormatDateToLong;
import table.temporay.Test_TemporayTable_Join_ProcessTime;

import java.util.LinkedList;
import java.util.List;

public class Test_HbaseSink {

    public static final Logger log = LoggerFactory.getLogger(Test_TemporayTable_Join_ProcessTime.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //setjobCheckpoint(env);
        //setjobRestartStrategy(env);

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
                + " rowtime as COALESCE(to_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),0),\n"
                + " watermark for rowtime as rowtime - interval '0' second\n"
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
                + "  'url' = 'jdbc:mysql://localhost:3306/local',\n"
                + "  'table-name' = 'rates',\n"
                + "  'username' = 'root',\n"
                + "  'password' = 'root',\n"
                + "  'lookup.cache.max-rows' = '100',\n"
                + "  'lookup.cache.ttl' = '10' "
                + ")";

        // upsertDDL
        String upsertDDL = "\n"
                + "create table agg_append_hbase (\n" +
                " rowkey string,\n" +
                " f1 row<cnt bigint,cnt_distinct_id bigint,ave_rate double>,\n" +
                " primary key (rowkey) not enforced\n" +
                ") with (\n" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'agg_table_hbase',\n" +
                " 'zookeeper.quorum' = 'bigdata01:2181'\n" +
                ")";

        List<String> list = new LinkedList<>();

        list.add(createKafkaDDL);
        list.add(createMysqlDDL);
        list.add(upsertDDL);

        for (String ddl : list) {
            log.info("执行sql：" + ddl);
            tEnv.executeSql(ddl);
        }

        String joinDML = "select o.id, o.currency,o.rowtime, r.rate\n" +
                "from \n" +
                "  orders as o left join rates for system_time as of o.proctime as r \n" +
                "on o.currency = r.currency";

        Table joinTable = tEnv.sqlQuery(joinDML);
        tEnv.createTemporaryView("joinTable", joinTable);

        String upsertDML = "select \n" +
                "    id,\n" +
                "    count(*) as cnt,\n" +
                "    count(distinct id) as cnt_distinct_id,\n" +
                "    avg(rate) as avg_rate,\n" +
                "    tumble_end(rowtime, interval '1' minute) as window_end\n" +
                "from joinTable\n" +
                "group by id,tumble(rowtime, interval '1' minute)";

        Table upsertTable = tEnv.sqlQuery(upsertDML);
        tEnv.createTemporaryView("upsertTable", upsertTable);
        tEnv.toAppendStream(upsertTable, Row.class).print();
        String appendHbaseDML = "select id||'_'||date_format(window_end,'yyyy-MM-dd'),Row(cnt,cnt_distinct_id,avg_rate) from upsertTable";
        Table appendSinkTable = tEnv.sqlQuery(appendHbaseDML);
        tEnv.toAppendStream(appendSinkTable, Row.class).print("appendSinkTable");
        appendSinkTable.executeInsert("agg_append_hbase");


        env.execute("TestTemporayTable");

    }
}
