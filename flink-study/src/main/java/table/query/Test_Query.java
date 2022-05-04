package table.query;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Test_Query {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String createKafkaDDL = "\n"
                + " create table orders (\n"
                + " id string,\n"
                + " currency string,\n"
                + " ts string,\n"
                + " proctime as proctime(),\n"
                + " rowtime as COALESCE(to_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),0),\n"
                + " watermark for rowtime as rowtime - interval '2' second\n"
                + ") with (\n"
                + "  'connector' = 'filesystem',\n"
                + "  'path' = 'file:///opt/workspace/javaproject/bigdata/flink-study/src/main/resources/data/orders.csv',\n"
                + "  'format' = 'csv',\n"
                + "  'csv.ignore-parse-errors' = 'true'\n"
                + ")";
        System.out.println(createKafkaDDL);
        tEnv.executeSql(createKafkaDDL);

        //tEnv.sqlQuery("" +
        //        "select * from orders order by rowtime" +
        //        "")
        //        .execute().print();
        //
        //
        //tEnv.sqlQuery("" +
        //        "select * from orders order by rowtime limit 3" +
        //        "")
        //        .execute().print();


        String query = "select id,currency,rowtime,rk from (select id,currency,rowtime,row_number() over(partition by id order by rowtime ) as rk from orders) t where t.rk = 1";
        tEnv.toRetractStream(tEnv.sqlQuery(query), Row.class)
        .print();

        tEnv.executeSql("DESCRIBE orders").print();
        tEnv.executeSql("explain plan for " + query).print();


        env.execute("test_query");
    }
}
