package table.source.socket;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestUdfTableSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)\n" +
                "WITH (\n" +
                "  'connector' = 'socket',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '9999',\n" +
                "  'byte-delimiter' = '10',\n" +
                "  'format' = 'changelog-csv',\n" +
                "  'changelog-csv.column-delimiter' = '|'\n" +
                ")");

        Table table = tEnv.sqlQuery("SELECT name, SUM(score) FROM UserScores GROUP BY name");

        tEnv.toRetractStream(table, Row.class).print();

        env.execute("TestUdfTableSource");
    }
}
