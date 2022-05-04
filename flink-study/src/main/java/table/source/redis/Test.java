package table.source.redis;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);


        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        DataStream<Tuple2<String,String>> map = dataStreamSource.map(line -> {
            String[] split = line.split(",");
            return new Tuple2<>(split[0], split[1]);
        }).returns(new TypeHint<Tuple2<String, String>>(){});

        tEnv.createTemporaryView("source", map, "v1,v2");

        map.print("source");

        tEnv.executeSql("CREATE TABLE redis_test (\n" +
                "  hashKey STRING,\n" +
                "  v1 STRING,\n" +
                "  v2 STRING,\n" +
                "  PRIMARY KEY (hashKey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'mode' = 'single',\n" +
                "  'single.host' = 'localhost',\n" +
                "  'single.port' = '6379',\n" +
                "  'db-num' = '0',\n" +
                "  'command' = 'HSET'\n" +
                ")");

        Table table = tEnv.sqlQuery("SELECT v1||'_'||v2 as hashKey,v1,v2 FROM source");
        tEnv.createTemporaryView("table1",table);
        tEnv.toAppendStream(table, Row.class);

        tEnv.executeSql("insert into  redis_test select hashKey,v1,v2 from table1");


        env.execute("TestRedisTableSink");
    }
}
