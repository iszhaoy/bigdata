package table.connectors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
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

import static common.FlinkSettings.setjobCheckpoint;
import static common.FlinkSettings.setjobRestartStrategy;

public class Test_ElasticSearchSink {

    public static final Logger log = LoggerFactory.getLogger(Test_TemporayTable_Join_ProcessTime.class);

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);

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
                + " watermark for rowtime as rowtime - interval '5' second\n"
                + ") with (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'Orders',\n"
                + "  'properties.group.id' = 'testGroup',\n"
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

        // upsert
        String aggWindowTableESDDL = "\n"
                + "create table agg_window_table_es (\n" +
                "  id string,\n" +
                "  cnt bigint,\n" +
                "  cnt_distinct bigint,\n" +
                "  average double,\n" +
                "  window_end timestamp(3),\n" +
                "  primary key (id,window_end) not enforced\n" +
                ") with (\n" +
                "    'connector' = 'elasticsearch-7', -- using elasticsearch connector\n" +
                "    'hosts' = 'http://127.0.0.1:9200',  -- elasticsearch address\n" +
                "    'index' = 'index_agg_window_table'  -- elasticsearch index name, similar to database table name\n" +
                ")\n";

        // append
        String aggTableESDDL = "\n"
                + "create table agg_table_es (\n" +
                "  id string,\n" +
                "  cnt bigint,\n" +
                "  cnt_distinct bigint,\n" +
                "  average double\n" +
                ") with (\n" +
                "    'connector' = 'elasticsearch-7', -- using elasticsearch connector\n" +
                "    'hosts' = 'http://127.0.0.1:9200',  -- elasticsearch address\n" +
                "    'index' = 'index_agg_table'  -- elasticsearch index name, similar to database table name\n" +
                ")\n";


        List<String> list = new LinkedList<>();

        list.add(createKafkaDDL);
        list.add(createMysqlDDL);
        list.add(aggWindowTableESDDL);
        list.add(aggTableESDDL);

        for (String ddl : list) {
            log.warn("执行sql：" + ddl);
            tEnv.executeSql(ddl);
        }

        String joinDML = "select o.id, o.currency,o.rowtime, r.rate\n" +
                "from \n" +
                "  orders as o left join rates for system_time as of o.proctime as r \n" +
                "on o.currency = r.currency";

        Table joinTable = tEnv.sqlQuery(joinDML);
        System.out.println(joinDML);
        tEnv.createTemporaryView("joinTable", joinTable);

        String aggWindowDML = "select \n" +
                "    id,\n" +
                "    count(*),\n" +
                "    count(distinct id) as count_distinct_id,\n" +
                "    avg(rate) as avg_rate,\n" +
                "    tumble_end(rowtime, interval '1' minute)\n" +
                "from joinTable\n" +
                "group by id,tumble(rowtime, interval '1' minute)";

        String aggDML = "select \n" +
                "    id,\n" +
                "    cast(count(id) as bigint) as cnt,\n" +
                "    cast(count(distinct id) as bigint) as cnt_distinct,\n" +
                "    cast(avg(rate) as double) as averge\n" +
                "from joinTable\n" +
                "group by id";

        System.out.println(aggWindowDML);
        System.out.println(aggDML);


        //Table aggWindowTable = tEnv.sqlQuery(aggWindowDML);
        //tEnv.toAppendStream(aggWindowTable, Row.class).print("aggWindowTable");
        ////aggWindowTable.executeInsert("agg_window_table_es");
        //
        //Table aggTable = tEnv.sqlQuery(aggDML);
        //// RetractStream 需要下游支持upsert
        //tEnv.toRetractStream(aggTable, Row.class).print("toRetractStream");
        //aggTable.executeInsert("agg_table_es");

        env.execute("TestTemporayTable");

    }
}
